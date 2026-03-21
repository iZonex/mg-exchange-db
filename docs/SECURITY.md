# ExchangeDB Security Guide

This guide covers authentication, authorization, encryption, and audit logging
in ExchangeDB.

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Role-Based Access Control (RBAC)](#role-based-access-control-rbac)
4. [Row-Level Security (RLS)](#row-level-security-rls)
5. [Encryption at Rest](#encryption-at-rest)
6. [TLS/SSL](#tlsssl)
7. [Audit Logging](#audit-logging)
8. [Service Accounts](#service-accounts)
9. [Security Best Practices](#security-best-practices)

---

## Overview

ExchangeDB provides a layered security model:

| Layer | Feature | Default |
|-------|---------|---------|
| Network | TLS/SSL encryption | Disabled |
| Authentication | Token, OAuth/OIDC, username/password | Disabled |
| Authorization | RBAC (users, roles, permissions) | Open access |
| Row-Level | Row-Level Security (RLS) | Disabled |
| Storage | Encryption at rest (ChaCha20-Poly1305) | Disabled |
| Audit | Audit logging (NDJSON) | Disabled |

By default, ExchangeDB starts with no authentication, allowing all clients
unrestricted access. This is suitable for development but must be configured
for production deployments.

---

## Authentication

### Token Authentication

The simplest authentication method. Configure a static token:

```toml
[security]
auth_enabled = true
token = "your-secret-token-here"
```

Clients include the token in the `Authorization` header:

```bash
curl -H 'Authorization: Bearer your-secret-token-here' \
  http://localhost:9000/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM trades LIMIT 5"}'
```

For psql connections, the token is passed as the password:

```bash
PGPASSWORD=your-secret-token-here psql -h localhost -p 8812 -d exchangedb
```

### OAuth 2.0 / OpenID Connect

ExchangeDB supports OAuth 2.0 / OIDC for enterprise authentication:

```toml
[security]
auth_enabled = true
oauth_issuer = "https://auth.example.com/realms/exchangedb"
```

#### OAuth Flow

1. User navigates to `GET /auth/login`.
2. ExchangeDB redirects to the OAuth provider's authorization endpoint.
3. User authenticates with the provider.
4. Provider redirects back to `GET /auth/callback` with an authorization code.
5. ExchangeDB exchanges the code for a JWT token.
6. Subsequent requests include the JWT in the `Authorization: Bearer` header.

#### Supported Providers

Any OAuth 2.0 / OIDC-compliant provider:

- Keycloak
- Auth0
- Okta
- Azure AD
- Google Workspace

#### JWT Validation

ExchangeDB validates JWT tokens against the issuer's JWKS (JSON Web Key Set):

- Signature verification.
- Expiration check (`exp` claim).
- Issuer validation (`iss` claim).
- Audience validation (`aud` claim).

### Username/Password Authentication

Users can authenticate with username and password via the admin API:

```bash
# Create a user
curl -X POST http://localhost:9000/admin/users \
  -H 'Content-Type: application/json' \
  -d '{"username": "alice", "password": "secure_password_123", "roles": ["reader"]}'
```

Passwords are hashed before storage (never stored in plaintext).

### ILP Authentication

When authentication is enabled, ILP TCP connections must authenticate using the
configured token. The token is sent as the first line of the TCP connection
before any ILP data.

### Auth Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/auth/login` | GET | Initiates OAuth login flow |
| `/auth/callback` | GET | OAuth callback (receives auth code) |
| `/auth/token` | GET | Returns current token info |
| `/auth/logout` | POST | Invalidates current session |

---

## Role-Based Access Control (RBAC)

ExchangeDB implements RBAC with users, roles, and permissions at the table
and column level.

### Users

```sql
-- Create a user
CREATE USER analyst WITH PASSWORD 'secret123';

-- Drop a user
DROP USER analyst;
```

Or via the admin API:

```bash
# Create user
curl -X POST http://localhost:9000/admin/users \
  -H 'Content-Type: application/json' \
  -d '{"username": "analyst", "password": "secret123", "roles": ["readonly"]}'

# List users
curl http://localhost:9000/admin/users

# Delete user
curl -X DELETE http://localhost:9000/admin/users/analyst
```

### Roles

```sql
-- Create a role
CREATE ROLE readonly;
CREATE ROLE trader;

-- Drop a role
DROP ROLE readonly;
```

Or via the admin API:

```bash
# Create role with permissions
curl -X POST http://localhost:9000/admin/roles \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "trader",
    "permissions": ["read", "write:trades", "write:orders"]
  }'

# List roles
curl http://localhost:9000/admin/roles
```

### Permissions

```sql
-- Grant table-level read permission
GRANT READ ON trades TO readonly;

-- Grant write permission
GRANT WRITE ON trades TO trader;

-- Grant role to user
GRANT readonly TO analyst;

-- Revoke permission
REVOKE READ ON trades FROM readonly;
```

### Permission Types

| Permission | SQL Syntax | Description |
|------------|-----------|-------------|
| Admin | `admin` | Full administrative access |
| DDL | `ddl` | CREATE, ALTER, DROP operations |
| System | `system` | System-level operations (VACUUM, etc.) |
| Read (all) | `read` | Query all tables |
| Read (table) | `read:<table>` | Query specific table |
| Write (all) | `write` | Insert/update/delete all tables |
| Write (table) | `write:<table>` | Insert/update/delete specific table |

### Permission Hierarchy

```
Admin
  |- DDL (CREATE, ALTER, DROP)
  |- System (VACUUM, SHOW, DESCRIBE)
  |- Read (all tables)
  |    |- Read (specific table)
  |- Write (all tables)
       |- Write (specific table)
```

### Example: Read-Only Analyst

```sql
CREATE ROLE analyst_role;
GRANT READ ON trades TO analyst_role;
GRANT READ ON quotes TO analyst_role;

CREATE USER analyst WITH PASSWORD 'analyst_pass';
GRANT analyst_role TO analyst;
```

The user `analyst` can query `trades` and `quotes` but cannot write to any
table or perform DDL operations.

### Example: Application Service Account

```sql
CREATE ROLE app_role;
GRANT READ ON trades TO app_role;
GRANT WRITE ON trades TO app_role;
GRANT READ ON quotes TO app_role;

CREATE USER trading_app WITH PASSWORD 'app_secret';
GRANT app_role TO trading_app;
```

### RBAC Storage

RBAC state (users, roles, permissions) is persisted to disk and survives
server restarts. The RBAC store is located in the data directory.

---

## Row-Level Security (RLS)

Row-Level Security restricts which rows a user can see based on their
identity or role.

### How RLS Works

1. RLS policies are defined per table.
2. When a user queries a table with RLS enabled, the engine transparently
   injects a WHERE clause filter.
3. The user sees only rows matching their RLS policy.
4. RLS is enforced in the query executor, so it applies to all access paths
   (SQL, HTTP, pgwire).

### Example

Consider a multi-tenant trades table:

```sql
CREATE TABLE trades (
    timestamp TIMESTAMP,
    tenant    SYMBOL,
    symbol    SYMBOL,
    price     DOUBLE,
    volume    DOUBLE
) TIMESTAMP(timestamp) PARTITION BY DAY;
```

With RLS configured, user `tenant_a` querying `SELECT * FROM trades` would
automatically have `WHERE tenant = 'tenant_a'` appended.

### Limitations

- RLS policies are configured programmatically, not via SQL.
- RLS applies to SELECT queries only (not INSERT/UPDATE/DELETE).

---

## Encryption at Rest

ExchangeDB supports encryption at rest using ChaCha20-Poly1305, an
authenticated encryption algorithm.

### Features

- **Authenticated encryption**: Detects data tampering.
- **Per-file encryption**: Each column file is individually encrypted.
- **Key management**: Keys are derived from a master secret.

### Configuration

Encryption at rest is enabled via configuration. The encryption key should
be provided securely (e.g., via environment variable, not in the config file):

```bash
export EXCHANGEDB_ENCRYPTION_KEY="your-256-bit-key-in-hex"
exchange-db server
```

### Considerations

- Encryption adds CPU overhead to reads and writes.
- Encrypted data is not compressible (encrypt after compression).
- Key rotation requires re-encryption of all data (offline operation).
- Backup files are also encrypted if the source data is encrypted.

---

## TLS/SSL

TLS encrypts data in transit between clients and the server.

### Configuration

```toml
[security]
tls_cert = "/etc/exchangedb/server.crt"
tls_key = "/etc/exchangedb/server.key"
```

### Generating Self-Signed Certificates

For development/testing:

```bash
openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt \
  -days 365 -nodes -subj "/CN=localhost"
```

### Using Let's Encrypt

For production with a public domain:

```bash
certbot certonly --standalone -d exchangedb.example.com

# Reference the certs in config
# tls_cert = "/etc/letsencrypt/live/exchangedb.example.com/fullchain.pem"
# tls_key = "/etc/letsencrypt/live/exchangedb.example.com/privkey.pem"
```

### Client Connection with TLS

```bash
# psql with SSL
psql "host=localhost port=8812 dbname=exchangedb sslmode=require"

# curl with HTTPS
curl https://localhost:9000/api/v1/health --cacert server.crt

# Python
conn = psycopg2.connect(
    host="localhost", port=8812, dbname="exchangedb",
    sslmode="require", sslrootcert="/path/to/ca.crt"
)
```

### TLS Scope

TLS is applied to:

- HTTP REST API (port 9000) -- becomes HTTPS.
- PostgreSQL wire protocol (port 8812) -- SSL/TLS handshake.

ILP (port 9009) does not currently support TLS. For secure ILP ingestion,
use ILP over HTTP with TLS enabled.

---

## Audit Logging

ExchangeDB records security-relevant events to an audit log.

### Audit Events

| Event Type | Description |
|------------|-------------|
| `auth_success` | Successful authentication |
| `auth_failure` | Failed authentication attempt |
| `permission_granted` | Authorization check passed |
| `permission_denied` | Authorization check failed |
| `ddl_execute` | DDL statement executed (CREATE, ALTER, DROP) |
| `data_access` | Data query executed |
| `admin_action` | Administrative action performed |

### Audit Log Format

NDJSON (Newline-Delimited JSON) with daily rotation:

```json
{"ts":"2024-03-21T14:30:00Z","event":"auth_success","user":"analyst","ip":"192.168.1.10"}
{"ts":"2024-03-21T14:30:01Z","event":"data_access","user":"analyst","table":"trades","query":"SELECT ..."}
{"ts":"2024-03-21T14:30:05Z","event":"permission_denied","user":"analyst","table":"admin_logs","action":"read"}
```

### Audit Log Location

Audit logs are written to the data directory with daily rotation:

```
/data/exchangedb/
  _audit/
    audit-2024-03-21.ndjson
    audit-2024-03-20.ndjson
    audit-2024-03-19.ndjson
```

### Integrating with SIEM

Export audit logs to your SIEM system:

```bash
# Ship to Elasticsearch
cat /data/exchangedb/_audit/audit-*.ndjson | \
  curl -X POST http://elasticsearch:9200/exchangedb-audit/_bulk \
    -H 'Content-Type: application/x-ndjson' --data-binary @-

# Ship to Splunk
cat /data/exchangedb/_audit/audit-*.ndjson | \
  curl -X POST https://splunk:8088/services/collector/raw \
    -H "Authorization: Splunk your-hec-token" --data-binary @-
```

---

## Service Accounts

Service accounts are designed for application-to-database authentication
without interactive login.

### Creating Service Accounts

```bash
curl -X POST http://localhost:9000/admin/users \
  -H 'Content-Type: application/json' \
  -d '{
    "username": "svc-trading-app",
    "password": "auto-generated-token",
    "roles": ["app_readwrite"]
  }'
```

### Best Practices for Service Accounts

1. Use a unique service account per application.
2. Grant minimal required permissions.
3. Rotate credentials periodically.
4. Monitor service account activity via audit logs.
5. Use descriptive names with a `svc-` prefix.

---

## Security Best Practices

### Production Checklist

1. **Enable authentication**: Set `auth_enabled = true`.
2. **Use strong tokens**: Generate tokens with at least 256 bits of entropy.
3. **Enable TLS**: Configure `tls_cert` and `tls_key`.
4. **Configure RBAC**: Create roles with minimal permissions.
5. **Enable audit logging**: Monitor access patterns.
6. **Network segmentation**: Place ExchangeDB behind a firewall.
7. **Bind to specific interfaces**: Avoid `0.0.0.0` in production.

### Token Generation

```bash
# Generate a secure random token
openssl rand -hex 32
```

### Network Security

```toml
[http]
bind = "10.0.1.5:9000"      # Bind to internal interface only

[pgwire]
bind = "10.0.1.5:8812"

[ilp]
bind = "10.0.1.5:9009"
```

### Firewall Rules

```bash
# Allow HTTP from application subnet only
iptables -A INPUT -p tcp --dport 9000 -s 10.0.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 9000 -j DROP

# Allow pgwire from application subnet
iptables -A INPUT -p tcp --dport 8812 -s 10.0.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 8812 -j DROP

# Allow ILP from ingestion servers
iptables -A INPUT -p tcp --dport 9009 -s 10.0.2.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 9009 -j DROP
```

### Regular Security Maintenance

1. **Rotate tokens** every 90 days.
2. **Review audit logs** weekly for anomalies.
3. **Update TLS certificates** before expiration.
4. **Review user permissions** quarterly.
5. **Keep ExchangeDB updated** for security patches.
6. **Monitor for unauthorized access** attempts in audit logs.
