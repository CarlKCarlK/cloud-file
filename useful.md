# Useful

```bash
cargo doc --no-deps --open
cargo check
cargo test
cargo test --doc
cargo publish --dry-run
```

## Publish

* Create new branch.
* Change version in Cargo.toml.

```bash
cargo install cargo-audit
cargo audit
cargo update
cargo test
cargo publish --dry-run
```

* Commit changes and check CI tests.
* Merge into main and commit (check CI tests again)

```bash
cargo publish
# tag version
git tag -a v0.2.0 -m "v0.2.0"
git push origin v0.2.0
```
