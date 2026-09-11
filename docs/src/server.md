---
icon: lucide/server
---

# Server

The optional `[server]` table configures the DataFusion query engine runtime.

```toml
[server]
memory-limit = "4GB"
flight-sql-server-port = 32010
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `memory-limit` | String | No | 80% of effective total memory | Caps the memory available to the query engine. |
| `flight-sql-server-port` | Integer | No | 32010 | Port the Arrow Flight SQL server started by `--flight-sql-server` listens on. |

The `memory-limit` value is an integer with an optional, case-insensitive unit. With no unit, the value is treated as bytes.

| Unit | Meaning |
| --- | --- |
| `b` | Bytes |
| `k`, `kb` | Kibibytes |
| `m`, `mb` | Mebibytes |
| `g`, `gb` | Gibibytes |
| `t`, `tb` | Tebibytes |