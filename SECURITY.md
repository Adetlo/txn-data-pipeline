# Security policy

## Scope

`txn-data-pipeline` is an independent educational project. It processes synthetic
financial transaction data only, is not affiliated with any bank or production system
and makes no compliance claims. The threat model is that of an open-source developer
tool: supply-chain issues in dependencies, unsafe handling of local files, and code
defects that could mislead someone who adapts the project for real data.

## Reporting a vulnerability

Open a GitHub security advisory on the repository
(**Security > Report a vulnerability**) or, for non-sensitive findings, a regular
GitHub issue. Please include the version or commit, reproduction steps and the impact
you see. There is no e-mail contact for this project.

Best effort response: within a couple of weeks. Fixes land on `main` and are noted in
the commit history; there are no backports.

## What is in scope

* Code under `src/`, `migrations/`, `scripts/`, the `Dockerfile` and CI workflows.
* Dependency vulnerabilities surfaced by `pip-audit` that affect the pipeline.

## What is out of scope

* Issues that require real financial data to be meaningful: the project has none.
* Hardening that a production deployment would need (encryption, access control,
  secret management); these are documented as not implemented in
  [docs/SECURITY.md](docs/SECURITY.md).
