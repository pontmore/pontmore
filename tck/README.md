# Pontmore TCK vectors

These vectors are executable implementation fixtures. Each case cites its
controlling specification section and declares the complete expected set of
stable error codes.

They currently exercise decoded PIP-00 and PIP-01 content and tags. They are
not yet normative protocol vectors because the protocol repository has not
adopted a vector format, and they do not include signed Nostr envelopes.

Run them with:

```console
cargo run -p pontmore-tck -- tck/vectors
```

The JSON format is language-neutral so another implementation can run the same
cases without depending on the Rust core.

