# Server

The optional `[server]` table configures the DataFusion query engine runtime.

```toml
[server]
memory-limit = "4GB"
```

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `memory-limit` | String | No | Unlimited | Caps the memory available to the query engine. |

The value is an integer with an optional, case-insensitive unit. With no unit, the value is treated as bytes.

| Unit | Meaning |
| --- | --- |
| `b` | Bytes |
| `k`, `kb` | Kibibytes |
| `m`, `mb` | Mebibytes |
| `g`, `gb` | Gibibytes |
| `t`, `tb` | Tebibytes |
