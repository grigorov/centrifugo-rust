# Go oracle

`build.sh` clones **centrifugo v2.8.6** and builds it into `bin/centrifugo`. This
is the real Go implementation, used as the differential **behavior oracle**: the
golden tests drive both it and the Rust binary with identical commands and
compare the replies (see `conformance/tests/m1_golden.rs`).

```
bash conformance/go-oracle/build.sh   # needs `go` on PATH
```

`bin/` and `src/` are git-ignored. If `go` is not found, prefix with
`PATH="$(brew --prefix)/bin:$PATH"`.

Note: the Go repo's own `*_test.go` files are in-process unit tests that link the
Go code as a library — they cannot run against the Rust binary. This oracle gives
us the next best thing: byte-level behavioral comparison over the real wire.
