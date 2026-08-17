# Security policy

## Scanner-only scope

The public binary is read-only and does not contain transaction signing or submission code. Do not add private keys, seed phrases, signer files or relay-auth keys to this repository.

RPC provider URLs may contain credentials. Keep them in `.env` or the process environment only. `.env` is ignored by Git.

The RPC transport layer intentionally sanitizes network errors so a request failure cannot trivially print a credential-bearing provider URL.

## Reporting an issue

For a security-sensitive issue, avoid posting credentials, private RPC URLs, wallet keys or exploitable secrets in a public GitHub issue. Reproduce with redacted/synthetic values whenever possible.

## Trading safety

Scanner output is not authorization to execute a transaction. Candidate economics can change between observation and inclusion, and the breadth-stage fee estimate is not an exact execution guarantee.
