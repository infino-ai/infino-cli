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

Releases are triggered manually from the Actions tab, not by pushing a tag
(`dispatch-releases = true` in `dist-workspace.toml`).

1. Bump `version` in `Cargo.toml` and merge it to `main`.
2. In the repo, open **Actions → Release → Run workflow**. Pick the branch to
   release from (usually `main`) and enter the tag in the **Release Tag** input
   (for example `v0.2.2`). Leave the default `dry-run` to only plan and build —
   nothing is tagged or published.
3. `dist` creates the `vX.Y.Z` tag on that commit, builds every target, creates
   the GitHub Release with checksummed artifacts, and publishes the Homebrew
   formula + npm package + crates.io.

Because the tag is created by the workflow, you no longer tag by hand — and a
tag pushed with `git` will **not** start a release.

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
