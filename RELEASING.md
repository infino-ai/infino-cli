# Releasing the infino CLI

The CLI lives in its own repo (`infino-ai/infino-cli`) and depends on the
published `infino` crate. It ships as prebuilt binaries via
[`dist`](https://github.com/axodotdev/cargo-dist) (cargo-dist): a shell
installer, a Homebrew formula, and an npm package, released on standard
`vX.Y.Z` tags.

## One-time setup

1. **Install dist:** `cargo install cargo-dist` (or the shell installer).
2. **Create the Homebrew tap repo:** `infino-ai/homebrew-tap` (public). `dist`
   pushes the generated formula there → `brew install infino-ai/tap/infino-cli`.
3. **Generate CI:** run `dist init` at the repo root. It validates
   `dist-workspace.toml`, pins the dist version, and **generates the release
   workflow** (`.github/workflows/release.yml`). Commit what it writes — do not
   hand-edit the generated workflow; re-run `dist init` to change it.
4. **Secrets** (GitHub repo settings):
   - npm publish token for `@infino-ai` (the npm publish job).
   - A token with **write access to the `homebrew-tap` repo** for the formula
     push — the default `GITHUB_TOKEN` is scoped to this repo only, so a PAT (or
     GitHub App token) is required for the cross-repo tap push. See the dist docs
     for the exact secret name expected by the generated workflow.

## Cutting a release

1. Bump `version` in `Cargo.toml`.
2. Tag and push:
   ```
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```
3. The `dist` workflow builds every target, creates the GitHub Release with
   checksummed artifacts, and publishes the Homebrew formula + npm package.

The CLI versions independently of the engine; bump `infino = "…"` in
`Cargo.toml` when adopting a newer engine release.

## Channels

- **shell:** `curl --proto '=https' --tlsv1.2 -LsSf <release-url>/installer.sh | sh`
- **Homebrew:** `brew install infino-ai/tap/infino-cli`
- **npm:** `npx @infino-ai/infino-cli` (binary: `infino`)

(Not published to crates.io — `dist` ships shell + npm + Homebrew. Add a
`cargo publish` step later if a `cargo install infino-cli` channel is wanted.)

## Agent skills

The release bundles nothing extra for skills — they are embedded in the binary
(`include_str!`), so `infino skills install` works from any install method.
