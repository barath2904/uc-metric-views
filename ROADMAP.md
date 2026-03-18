# Roadmap

## v0.2 — Developer Experience

### Dynamic versioning (`hatch-vcs`)

Replace the hardcoded `version = "0.1.0"` in `pyproject.toml` with version derived from git tags. The tag becomes the single source of truth — no more manual version bumps before releasing.

```toml
[tool.hatch.version]
source = "vcs"
```

### Automated releases (`release-please`)

Add the `release-please` GitHub Actions workflow. It reads conventional commits (`feat:`, `fix:`, `chore:`) and automatically:
- Opens a PR bumping the version and updating `CHANGELOG.md`
- Creates the GitHub Release when the PR is merged, triggering `release.yml`

Replaces the manual `git tag vX.Y.Z && git push --tags` step.

### OAuth M2M CLI flags

Add `--client-id` / `--client-secret` CLI flags to `generate`, `inspect`, and `deploy` commands as an explicit alternative to env vars. Currently OAuth M2M works via `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` env vars (SDK auto-detects), but there are no corresponding CLI flags — only `--token`.

### Databricks OIDC federation for GitHub Actions

Databricks supports GitHub Actions OIDC identity federation, eliminating the need to store `DATABRICKS_CLIENT_SECRET` in GitHub secrets entirely — similar to how PyPI publishing uses OIDC. Document the setup pattern and add a reference workflow.

### Collapse release workflows

Once 2–3 releases have been made and the publish process is proven, merge `release.yml` and `publish.yml` back into a single workflow: tag → TestPyPI → PyPI sequential. See comments in both files.

---

## Future

- Window measures scaffolding in `ucm generate`
- Materialization config scaffolding
- Snowflake join detection heuristics
- Semantic metadata prompts (`display_name`, `synonyms`) during generation
