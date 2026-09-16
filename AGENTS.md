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

## Creating and publishing the branch

The mechanics matter as much as the intent: the default way of starting a branch
from `main` silently aims every later push at `main`.

- Create the branch: 

      git fetch origin main
      git checkout -b <feature 

- Check the destination before the first push to a shared remote:

      git rev-parse --abbrev-ref '@{upstream}'   # must not be a shared branch
      git push --dry-run origin HEAD             # prints the real destination

- Publish with an explicit destination:

      git push -u origin refs/heads/<branch>:refs/heads/<branch>

This repository sets `push.default=current` locally so a branch name cannot
resolve to a shared branch. Leave it in place.

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
