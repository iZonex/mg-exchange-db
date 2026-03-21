//! Built-in web console for ExchangeDB.
//!
//! Serves a single-page application at `GET /` that provides an interactive
//! SQL editor, table browser, and result viewer -- similar to QuestDB's
//! built-in console. All HTML, CSS, and JavaScript is embedded as a const
//! string so there are no external dependencies or static file directories.

use axum::response::Html;

/// Handler for `GET /` -- returns the embedded web console.
pub fn console_handler() -> Html<&'static str> {
    Html(CONSOLE_HTML)
}

const CONSOLE_HTML: &str = r#"<!DOCTYPE html>
<html>
<head>
    <title>ExchangeDB Console</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Segoe UI', monospace; background: #1e1e2e; color: #cdd6f4; }
        .header { background: #181825; padding: 12px 20px; display: flex; align-items: center; gap: 16px; border-bottom: 1px solid #313244; }
        .header h1 { font-size: 18px; color: #89b4fa; }
        .header .version { color: #6c7086; font-size: 12px; }
        .main { display: flex; height: calc(100vh - 48px); }
        .sidebar { width: 240px; background: #181825; border-right: 1px solid #313244; padding: 12px; overflow-y: auto; }
        .sidebar h3 { color: #a6adc8; font-size: 12px; text-transform: uppercase; margin-bottom: 8px; }
        .table-item { padding: 6px 8px; cursor: pointer; border-radius: 4px; font-size: 13px; }
        .table-item:hover { background: #313244; }
        .editor-area { flex: 1; display: flex; flex-direction: column; }
        .editor { flex: 0 0 200px; background: #11111b; border-bottom: 1px solid #313244; }
        .editor textarea { width: 100%; height: 100%; background: transparent; color: #cdd6f4; border: none; padding: 16px; font-family: 'Fira Code', monospace; font-size: 14px; resize: none; outline: none; }
        .toolbar { background: #181825; padding: 8px 16px; display: flex; gap: 8px; align-items: center; }
        .btn { background: #89b4fa; color: #1e1e2e; border: none; padding: 6px 16px; border-radius: 4px; cursor: pointer; font-weight: 600; font-size: 13px; }
        .btn:hover { background: #74c7ec; }
        .btn-secondary { background: #313244; color: #cdd6f4; }
        .results { flex: 1; overflow: auto; padding: 0; }
        .results table { width: 100%; border-collapse: collapse; font-size: 13px; }
        .results th { background: #181825; color: #89b4fa; padding: 8px 12px; text-align: left; position: sticky; top: 0; border-bottom: 2px solid #313244; }
        .results td { padding: 6px 12px; border-bottom: 1px solid #313244; }
        .results tr:hover { background: #313244; }
        .status { background: #181825; padding: 6px 16px; font-size: 12px; color: #6c7086; border-top: 1px solid #313244; }
        .error { color: #f38ba8; }
        .timing { color: #a6e3a1; }
    </style>
</head>
<body>
    <div class="header">
        <h1>ExchangeDB</h1>
        <span class="version">v0.1.0</span>
    </div>
    <div class="main">
        <div class="sidebar" id="sidebar">
            <h3>Tables</h3>
            <div id="tables-list">Loading...</div>
        </div>
        <div class="editor-area">
            <div class="editor">
                <textarea id="sql" placeholder="Enter SQL query...">SELECT * FROM trades LIMIT 100</textarea>
            </div>
            <div class="toolbar">
                <button class="btn" onclick="executeQuery()">Run (Ctrl+Enter)</button>
                <button class="btn btn-secondary" onclick="clearResults()">Clear</button>
                <span id="timing" class="timing"></span>
            </div>
            <div class="results" id="results"></div>
            <div class="status" id="status">Ready</div>
        </div>
    </div>
    <script>
        // Load tables on start
        fetch('/api/v1/tables').then(r => r.json()).then(data => {
            const list = document.getElementById('tables-list');
            list.innerHTML = (data.tables || []).map(t =>
                `<div class="table-item" onclick="describeTable('${t}')">${t}</div>`
            ).join('') || '<div style="color:#6c7086">No tables</div>';
        });

        function describeTable(name) {
            document.getElementById('sql').value = `SELECT * FROM ${name} LIMIT 100`;
            executeQuery();
        }

        function clearResults() {
            document.getElementById('results').innerHTML = '';
            document.getElementById('timing').textContent = '';
            document.getElementById('status').textContent = 'Ready';
        }

        function executeQuery() {
            const sql = document.getElementById('sql').value.trim();
            if (!sql) return;
            document.getElementById('status').textContent = 'Executing...';
            document.getElementById('timing').textContent = '';

            fetch('/api/v1/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query: sql })
            })
            .then(r => r.json())
            .then(data => {
                // Show server-side query execution time (not HTTP round-trip)
                if (data.timing_ms !== undefined) {
                    const t = data.timing_ms;
                    let text;
                    if (t < 0.001) text = `${(t * 1000000).toFixed(0)}ns`;
                    else if (t < 1) text = `${(t * 1000).toFixed(0)}\u00b5s`;
                    else if (t < 1000) text = `${t.toFixed(2)}ms`;
                    else text = `${(t / 1000).toFixed(2)}s`;
                    document.getElementById('timing').textContent = text;
                }

                if (data.error) {
                    document.getElementById('results').innerHTML = `<div style="padding:16px" class="error">${data.error}</div>`;
                    document.getElementById('status').textContent = 'Error';
                    return;
                }

                if (data.columns && data.rows) {
                    let html = '<table><tr>' + data.columns.map(c => `<th>${c.name || c}</th>`).join('') + '</tr>';
                    data.rows.forEach(row => {
                        html += '<tr>' + row.map(v => `<td>${v === null ? '<span style="color:#6c7086">NULL</span>' : v}</td>`).join('') + '</tr>';
                    });
                    html += '</table>';
                    document.getElementById('results').innerHTML = html;
                    document.getElementById('status').textContent = `${data.rows.length} row(s) returned`;
                } else {
                    document.getElementById('results').innerHTML = `<div style="padding:16px;color:#a6e3a1">OK${data.affected_rows !== undefined ? ` (${data.affected_rows} rows affected)` : ''}</div>`;
                    document.getElementById('status').textContent = 'OK';
                    // Refresh tables list
                    fetch('/api/v1/tables').then(r => r.json()).then(d => {
                        const list = document.getElementById('tables-list');
                        list.innerHTML = (d.tables || []).map(t => `<div class="table-item" onclick="describeTable('${t}')">${t}</div>`).join('');
                    });
                }
            })
            .catch(err => {
                document.getElementById('results').innerHTML = `<div style="padding:16px" class="error">${err}</div>`;
                document.getElementById('status').textContent = 'Connection error';
            });
        }

        // Ctrl+Enter to execute
        document.getElementById('sql').addEventListener('keydown', e => {
            if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); executeQuery(); }
        });
    </script>
</body>
</html>"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_html_contains_required_elements() {
        assert!(CONSOLE_HTML.contains("<!DOCTYPE html>"));
        assert!(CONSOLE_HTML.contains("<title>ExchangeDB Console</title>"));
        assert!(CONSOLE_HTML.contains("id=\"sql\""));
        assert!(CONSOLE_HTML.contains("id=\"results\""));
        assert!(CONSOLE_HTML.contains("id=\"status\""));
        assert!(CONSOLE_HTML.contains("id=\"tables-list\""));
        assert!(CONSOLE_HTML.contains("executeQuery()"));
        assert!(CONSOLE_HTML.contains("/api/v1/query"));
        assert!(CONSOLE_HTML.contains("/api/v1/tables"));
        assert!(CONSOLE_HTML.contains("Ctrl+Enter"));
    }

    #[test]
    fn console_handler_returns_html() {
        let response = console_handler();
        let body = response.0;
        assert!(body.starts_with("<!DOCTYPE html>"));
        assert!(body.contains("</html>"));
    }
}
