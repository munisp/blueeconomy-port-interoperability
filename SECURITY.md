# Security Posture — blueeconomy-port-interoperability

Phase 11 security audit (branch `phase11/security`).

## Controls verified
- **Secrets**: working-tree scan clean.
- **AuthN/Z**: all `/v1/` routes behind `requireAuthentication` + tenant middleware; NSW ingress authenticated separately; USSD callback authenticated (`POST /ussd/callback` wraps authenticator). Tenant sweeper is tenant-scoped.
- **Injection**: parameterized pgx; the only `fmt.Sprintf` hits are event keys, not SQL.
- **SSRF**: outbound clients (customs, mojaloop, NSW, JWKS, token source) use config-driven base URLs, `url.PathEscape` on path params, redirect-following disabled, bounded response reads.
- **RLS**: complete — every tenant-scoped table (0008/0020 pattern through 0020 push_tokens) has ENABLE+FORCE RLS with default-deny tenant policies. Verified all 32 tables.

## Fixes this phase
- None required; posture confirmed by audit.

## Residuals
- JWKS/token endpoints are operator-configured URLs; protect config integrity (GitOps review) as the SSRF control plane.
