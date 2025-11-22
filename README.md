# Caddy Storage Provider for KV Database

A Caddy storage module that uses the Enzonix Key Value Database HTTP API as a storage backend. This allows Caddy to store TLS certificates, OCSP staples, and other data in your distributed key-value database.

**Repository:** [https://github.com/Enzonix-LLC/kv-caddy](https://github.com/Enzonix-LLC/kv-caddy)

## Caddy Module Name

```
caddy.storage.enzonix_kv
```

## Build Custom Caddy with This Storage

Use `xcaddy` to build a custom Caddy binary with this storage module:

```bash
xcaddy build --with github.com/Enzonix-LLC/kv-caddy
```

## Configuration

### Basic Setup

Add the storage configuration to your Caddyfile:

```
{
    storage enzonix_kv {
        endpoint "https://region-code.kv.enzonix.com"
        namespace "user:db"
        api_key "your-api-key-here"
    }
}
```

### Using Environment Variables

You can use environment variable placeholders in the Caddyfile:

```
{
    storage enzonix_kv {
        endpoint "{env.KV_DATABASE_ENDPOINT}"
        namespace "{env.KV_DATABASE_NAMESPACE}"
        api_key "{env.KV_DATABASE_API_KEY}"
    }
}
```

Caddy will replace `{env.VARIABLE_NAME}` with the value of the `VARIABLE_NAME` environment variable at runtime.

### Example: Using with TLS

Here's a complete example using the kv-database storage with TLS certificates:

```
{
    http_port 80
    https_port 443

    storage enzonix_kv {
        endpoint "https://region-code.kv.enzonix.com"
        namespace "caddy:certificates"
        api_key "{env.KV_DATABASE_API_KEY}"
    }
}

example.org, *.example.org {
    reverse_proxy localhost:3000

    tls {
        # Certificates will be stored in kv-database
    }
}
```

## Configuration Options

- `endpoint` (optional): The base URL of your kv-database server. Defaults to `https://region-code.kv.enzonix.com`.
- `namespace` (required): The namespace to use for storing Caddy data. Format: `{user_id}:{database_id}` (e.g., `user1:db1`).
- `api_key` (required): The API key for authenticating with the kv-database API.

## API Key Setup

Before using this storage module, you need to:

1. Create a user and database in your kv-database instance
2. Generate an API key for that database
3. Use the API key in your Caddy configuration

See the [kv-database README](https://github.com/Enzonix-LLC/kv-database) for instructions on creating users, databases, and API keys.

## How It Works

The storage module makes HTTP requests to your kv-database API:

- **Store**: `POST /api/write/{namespace}/{key}` with `{"value": "<base64-encoded-data>"}`
- **Load**: `GET /api/read/{namespace}/{key}` returns `{"key": "...", "value": "<base64-encoded-data>"}`
- **Delete**: `DELETE /api/write/{namespace}/{key}`
- **List**: `GET /api/read/{namespace}` returns `{"namespace": "...", "keys": [...]}`

All values are base64-encoded before storage to handle binary data (like TLS certificates).

## Notes

- The storage module uses a 30-second timeout for HTTP requests
- Modification times and file sizes are not available from the kv-database API, so `Stat()` returns basic information only
- All operations require proper authentication via the API key
- The namespace must match the format expected by your kv-database instance

