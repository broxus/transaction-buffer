# transaction-buffer

## Logging (tracing)

`transaction-buffer` uses `tracing`.

Optional Cargo feature:

- `compact-logs` reduces TTY log noise
  - removes extra span fields emitted by this library

Example:

```bash
cargo test --features compact-logs
```
