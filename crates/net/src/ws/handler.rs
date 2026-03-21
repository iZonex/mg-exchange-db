use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::sync::RwLock;

use crate::http::handlers::AppState;

/// Key for a subscription: (table_name, optional symbol filter).
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct SubscriptionKey {
    pub table: String,
    pub symbol: Option<String>,
}

/// A single broadcast event: JSON rows written to a table.
#[derive(Debug, Clone, Serialize)]
pub struct WriteEvent {
    pub table: String,
    pub rows: Vec<serde_json::Value>,
}

/// Manages broadcast channels for real-time data streaming.
///
/// Each unique (table, symbol) combination gets its own broadcast channel.
/// Clients subscribe by sending JSON messages over their WebSocket connection.
pub struct SubscriptionManager {
    /// Map of subscription keys to broadcast senders.
    channels: RwLock<HashMap<SubscriptionKey, broadcast::Sender<WriteEvent>>>,
}

impl SubscriptionManager {
    /// Create a new empty subscription manager.
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a broadcast sender for the given key.
    pub async fn get_or_create_sender(
        &self,
        key: &SubscriptionKey,
    ) -> broadcast::Sender<WriteEvent> {
        // Fast path: read lock.
        {
            let channels = self.channels.read().await;
            if let Some(sender) = channels.get(key) {
                return sender.clone();
            }
        }

        // Slow path: write lock to insert.
        let mut channels = self.channels.write().await;
        // Double-check after acquiring write lock.
        if let Some(sender) = channels.get(key) {
            return sender.clone();
        }

        let (tx, _rx) = broadcast::channel(256);
        channels.insert(key.clone(), tx.clone());
        tx
    }

    /// Subscribe to a (table, symbol) pair, returning a broadcast receiver.
    pub async fn subscribe(
        &self,
        table: String,
        symbol: Option<String>,
    ) -> broadcast::Receiver<WriteEvent> {
        let key = SubscriptionKey { table, symbol };
        let sender = self.get_or_create_sender(&key).await;
        sender.subscribe()
    }

    /// Broadcast a write event to all subscribers of the given table.
    ///
    /// This sends to both table-level subscribers (no symbol filter) and
    /// symbol-specific subscribers if the rows contain matching symbols.
    pub async fn broadcast(&self, table: &str, rows: Vec<serde_json::Value>) {
        let channels = self.channels.read().await;

        let event = WriteEvent {
            table: table.to_string(),
            rows: rows.clone(),
        };

        // Send to table-level subscribers (no symbol filter).
        let table_key = SubscriptionKey {
            table: table.to_string(),
            symbol: None,
        };
        if let Some(sender) = channels.get(&table_key) {
            // Ignore send errors (no receivers).
            let _ = sender.send(event.clone());
        }

        // Send to symbol-specific subscribers.
        // Extract symbol from rows if present.
        for (key, sender) in channels.iter() {
            if key.table == table {
                if let Some(ref symbol) = key.symbol {
                    // Filter rows that match this symbol.
                    let matching: Vec<serde_json::Value> = rows
                        .iter()
                        .filter(|row| {
                            row.get("symbol")
                                .and_then(|v| v.as_str())
                                .is_some_and(|s| s == symbol)
                        })
                        .cloned()
                        .collect();

                    if !matching.is_empty() {
                        let filtered_event = WriteEvent {
                            table: table.to_string(),
                            rows: matching,
                        };
                        let _ = sender.send(filtered_event);
                    }
                }
            }
        }
    }

    /// Return the number of active channels (for testing).
    pub async fn channel_count(&self) -> usize {
        self.channels.read().await.len()
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SubscriptionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionManager").finish_non_exhaustive()
    }
}

/// Client-to-server WebSocket message.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ClientMessage {
    Subscribe {
        subscribe: String,
        symbol: Option<String>,
    },
    Unsubscribe {
        unsubscribe: String,
    },
}

/// Server-to-client acknowledgement.
#[derive(Debug, Serialize)]
struct AckMessage {
    status: &'static str,
    action: String,
    table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol: Option<String>,
}

/// Server-to-client error.
#[derive(Debug, Serialize)]
struct ErrorMessage {
    error: String,
}

/// `GET /api/v1/ws` — WebSocket upgrade handler.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Handle an individual WebSocket connection.
async fn handle_socket(socket: WebSocket, state: Arc<AppState>) {
    let (mut ws_sender, mut ws_receiver) = socket.split();

    use futures::SinkExt;
    use futures::StreamExt;

    // Active subscriptions for this client: key -> abort handle for the forwarding task.
    let mut active_subs: HashMap<SubscriptionKey, tokio::task::JoinHandle<()>> = HashMap::new();

    // Channel for forwarding broadcast events to the WebSocket sender.
    let (fwd_tx, mut fwd_rx) = tokio::sync::mpsc::channel::<String>(256);

    // Spawn a task that forwards messages from the mpsc channel to the WebSocket.
    let send_task = tokio::spawn(async move {
        while let Some(msg) = fwd_rx.recv().await {
            if ws_sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    // Process incoming messages from the client.
    while let Some(Ok(msg)) = ws_receiver.next().await {
        match msg {
            Message::Text(text) => {
                let text_str: &str = &text;
                match serde_json::from_str::<ClientMessage>(text_str) {
                    Ok(ClientMessage::Subscribe { subscribe, symbol }) => {
                        let key = SubscriptionKey {
                            table: subscribe.clone(),
                            symbol: symbol.clone(),
                        };

                        // Don't duplicate subscriptions.
                        if active_subs.contains_key(&key) {
                            let ack = AckMessage {
                                status: "ok",
                                action: "already_subscribed".to_string(),
                                table: subscribe,
                                symbol,
                            };
                            let _ = fwd_tx
                                .send(serde_json::to_string(&ack).unwrap())
                                .await;
                            continue;
                        }

                        let mut rx = state
                            .subscriptions
                            .subscribe(subscribe.clone(), symbol.clone())
                            .await;

                        let fwd_tx_clone = fwd_tx.clone();

                        // Spawn a task that reads from the broadcast receiver and
                        // forwards to the WebSocket via the mpsc channel.
                        let handle = tokio::spawn(async move {
                            loop {
                                match rx.recv().await {
                                    Ok(event) => {
                                        let json =
                                            serde_json::to_string(&event).unwrap_or_default();
                                        if fwd_tx_clone.send(json).await.is_err() {
                                            break;
                                        }
                                    }
                                    Err(broadcast::error::RecvError::Lagged(n)) => {
                                        tracing::warn!(
                                            skipped = n,
                                            "WebSocket subscriber lagged"
                                        );
                                    }
                                    Err(broadcast::error::RecvError::Closed) => break,
                                }
                            }
                        });

                        active_subs.insert(key, handle);

                        let ack = AckMessage {
                            status: "ok",
                            action: "subscribed".to_string(),
                            table: subscribe,
                            symbol,
                        };
                        let _ = fwd_tx
                            .send(serde_json::to_string(&ack).unwrap())
                            .await;
                    }
                    Ok(ClientMessage::Unsubscribe { unsubscribe }) => {
                        // Unsubscribe from all symbol variants for this table.
                        let keys_to_remove: Vec<SubscriptionKey> = active_subs
                            .keys()
                            .filter(|k| k.table == unsubscribe)
                            .cloned()
                            .collect();

                        for key in &keys_to_remove {
                            if let Some(handle) = active_subs.remove(key) {
                                handle.abort();
                            }
                        }

                        let ack = AckMessage {
                            status: "ok",
                            action: "unsubscribed".to_string(),
                            table: unsubscribe,
                            symbol: None,
                        };
                        let _ = fwd_tx
                            .send(serde_json::to_string(&ack).unwrap())
                            .await;
                    }
                    Err(e) => {
                        let err = ErrorMessage {
                            error: format!("invalid message: {e}"),
                        };
                        let _ = fwd_tx
                            .send(serde_json::to_string(&err).unwrap())
                            .await;
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    // Clean up: abort all subscription forwarding tasks.
    for (_, handle) in active_subs {
        handle.abort();
    }
    drop(fwd_tx);
    let _ = send_task.await;
}

/// Notify all subscribers that new rows were written to a table.
///
/// Called from the write handler after successfully writing data.
pub async fn notify_write(
    subscriptions: &SubscriptionManager,
    table: &str,
    rows: Vec<serde_json::Value>,
) {
    subscriptions.broadcast(table, rows).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscription_manager_subscribe() {
        let mgr = SubscriptionManager::new();

        let mut rx = mgr.subscribe("trades".to_string(), None).await;

        // Broadcast an event.
        let rows = vec![serde_json::json!({"price": 100.0, "symbol": "BTC/USD"})];
        mgr.broadcast("trades", rows.clone()).await;

        let event = rx.recv().await.unwrap();
        assert_eq!(event.table, "trades");
        assert_eq!(event.rows.len(), 1);
    }

    #[tokio::test]
    async fn test_subscription_manager_symbol_filter() {
        let mgr = SubscriptionManager::new();

        let mut rx_btc = mgr
            .subscribe("trades".to_string(), Some("BTC/USD".to_string()))
            .await;
        let mut rx_eth = mgr
            .subscribe("trades".to_string(), Some("ETH/USD".to_string()))
            .await;

        let rows = vec![
            serde_json::json!({"price": 100.0, "symbol": "BTC/USD"}),
            serde_json::json!({"price": 50.0, "symbol": "ETH/USD"}),
        ];
        mgr.broadcast("trades", rows).await;

        let btc_event = rx_btc.recv().await.unwrap();
        assert_eq!(btc_event.rows.len(), 1);
        assert_eq!(btc_event.rows[0]["symbol"], "BTC/USD");

        let eth_event = rx_eth.recv().await.unwrap();
        assert_eq!(eth_event.rows.len(), 1);
        assert_eq!(eth_event.rows[0]["symbol"], "ETH/USD");
    }

    #[tokio::test]
    async fn test_subscription_manager_no_match() {
        let mgr = SubscriptionManager::new();

        let mut rx = mgr
            .subscribe("trades".to_string(), Some("DOGE/USD".to_string()))
            .await;

        let rows = vec![serde_json::json!({"price": 100.0, "symbol": "BTC/USD"})];
        mgr.broadcast("trades", rows).await;

        // No matching event should be received; use try_recv to avoid blocking.
        // Give a tiny window for the broadcast to propagate.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_subscription_manager_different_table() {
        let mgr = SubscriptionManager::new();

        let mut rx = mgr.subscribe("orders".to_string(), None).await;

        let rows = vec![serde_json::json!({"price": 100.0})];
        mgr.broadcast("trades", rows).await;

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_channel_count() {
        let mgr = SubscriptionManager::new();

        let _rx1 = mgr.subscribe("trades".to_string(), None).await;
        let _rx2 = mgr
            .subscribe("trades".to_string(), Some("BTC/USD".to_string()))
            .await;
        let _rx3 = mgr.subscribe("orders".to_string(), None).await;

        assert_eq!(mgr.channel_count().await, 3);
    }

    #[tokio::test]
    async fn test_notify_write() {
        let mgr = SubscriptionManager::new();
        let mut rx = mgr.subscribe("trades".to_string(), None).await;

        let rows = vec![serde_json::json!({"price": 42.0})];
        notify_write(&mgr, "trades", rows).await;

        let event = rx.recv().await.unwrap();
        assert_eq!(event.table, "trades");
        assert_eq!(event.rows[0]["price"], 42.0);
    }
}
