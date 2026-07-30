# Releasing the infino CLI

The CLI lives in its own repo (`infino-ai/infino-cli`) and depends on the
published `infino` crate. It ships as prebuilt binaries via
[`dist`](https://github.com/axodotdev/cargo-dist) (cargo-dist): a shell
installer, a Homebrew formula, and an npm package. A release is started by
**publishing a GitHub Release** for a `vX.Y.Z` tag; dist then builds the
binaries and uploads them to that Release (`create-release = false`).

## One-time setup

1. **Install dist:** `cargo install cargo-dist` (or the shell installer).
2. **Create the Homebrew tap repo:** `infino-ai/homebrew-tap` (public). `dist`
   pushes the generated formula there → `brew install infino-ai/tap/infino-cli`.
3. **Generate CI:** run `dist init` (or `dist generate`) at the repo root. It
   validates `dist-workspace.toml`, pins the dist version, and **generates the
   release workflow** (`.github/workflows/release.yml`). One deliberate
   exception to "don't hand-edit": the `on:` trigger block and the
   `github.event.release.tag_name` references are hand-edited so the workflow
   fires on a published GitHub Release rather than a tag push (dist has no
   config for that trigger). After any regenerate, re-apply that block — it's
   flagged with a `HAND-EDITED` comment.
4. **Secrets** (GitHub repo settings):
   - npm publish token for `@infino-ai` (the npm publish job).
   - A token with **write access to the `homebrew-tap` repo** for the formula
     push — the default `GITHUB_TOKEN` is scoped to this repo only, so a PAT (or
     GitHub App token) is required for the cross-repo tap push. See the dist docs
     for the exact secret name expected by the generated workflow.

## Cutting a release

1. Bump `version` in `Cargo.toml` and merge it to `main`.
2. On GitHub, **Releases → Draft a new release**. Create a new tag `vX.Y.Z`
   targeting `main`, write the notes, and click **Publish release**.
3. Publishing fires the workflow: it builds every target, uploads the
   checksummed artifacts to that Release, and publishes the Homebrew formula +
   npm package + crates.io.

The Release goes live immediately (before the binaries finish building, which
takes a while for this crate); the artifacts attach a few minutes later when the
run completes. Publishing the Release is what creates the tag, so don't create
the tag separately first.

The CLI versions independently of the engine; bump `infino = "…"` in
`Cargo.toml` when adopting a newer engine release.

## Channels

- **shell:** `curl --proto '=https' --tlsv1.2 -LsSf <release-url>/installer.sh | sh`
- **Homebrew:** `brew install infino-ai/tap/infino-cli`
- **npm:** `npx @infino-ai/infino-cli` (binary: `infino`)
- **cargo:** `cargo install infino-cli`

All four ship from one tag: crates.io via the `./publish-crates-io` custom job
(`CARGO_REGISTRY_TOKEN`), the rest via `dist`.

## Agent skills

The release bundles nothing extra for skills — they are embedded in the binary
(`include_str!`), so `infino skills install` works from any install method.
