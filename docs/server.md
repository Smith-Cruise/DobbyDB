# Server

The optional `[server]` table contains settings that apply to the query engine.
Currently, DobbyDB exposes only the memory limit.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `memory-limit` | String | No | No explicit limit | Caps the memory available to the DataFusion runtime. |

The value is an integer followed by an optional, case-insensitive unit:

| Unit | Meaning |
| --- | --- |
| `b` | Bytes |
| `k`, `kb` | Kibibytes (`2^10` bytes) |
| `m`, `mb` | Mebibytes (`2^20` bytes) |
| `g`, `gb` | Gibibytes (`2^30` bytes) |
| `t`, `tb` | Tebibytes (`2^40` bytes) |

A value without a unit is interpreted as bytes. Whitespace around the complete
value is ignored.

```toml
[server]
memory-limit = "4GB"
```

Invalid numeric values, unsupported suffixes, and values that overflow the
platform's `usize` range cause configuration loading to fail.
