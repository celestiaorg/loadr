# Branching and integration policy

## Branch roles

- `main` is the upstream baseline for independent work. Do not commit project
  changes directly to it.
- `risotto` is the integration and analysis branch. It should contain an
  up-to-date `main` plus all completed work that has not yet merged upstream.
- NEVER base implementation, fixes, features, performance work, refactoring,
  documentation, maintenance, or automation on `risotto` or `origin/risotto`.
- Do not commit content changes directly to `risotto`. Only merge commits and
  conflict resolutions required to integrate branches belong there.

## Starting work

- Before making changes, use Git history to identify the branch that owns the
  affected functionality.
- Start independent work in a new branch based on an up-to-date `main`.
- Start dependent work in a new branch based on the owning feature branch.
- A small, tightly related follow-up may be folded into its owning feature
  branch instead of creating a child branch. Never fold it directly into
  `main`.

## Completing and integrating work

- Test and review changes on their owning branch.
- Merge completed work into `risotto`.
- Periodically merge an up-to-date `main` into `risotto`.
- If integration exposes a defect, repair or reconstruct the owning feature
  branch first, then merge it again. Do not create a `risotto`-based cleanup
  branch.
- Perform cross-feature analysis on `risotto`, because it contains the latest
  integrated advancements. Keep analysis read-only; implement any resulting
  changes on the appropriate owning branch.
