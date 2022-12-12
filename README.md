# Flight arrow gRPC service experimentation

The objective of this repo is to test the Flight arrow gRPC server in a couple of settings:
1. A service that exposes some parquet datasets and that also accepts writing parquet datasets to a storage.
2. A read-only service on top of Delta Lake tables, that uses the `deltalake` package build on top of `delta-rs` to use the benefit of flight on top of Delta Tables.