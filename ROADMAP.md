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

### Collapse release workflows

Once 2–3 releases have been made and the publish process is proven, merge `release.yml` and `publish.yml` back into a single workflow: tag → TestPyPI → PyPI sequential. See comments in both files.

---

## Future

- Window measures scaffolding in `ucm generate`
- Materialization config scaffolding
- Snowflake join detection heuristics
- Semantic metadata prompts (`display_name`, `synonyms`) during generation
