# Restoring the git history

`refundsplit/` ships with its `.git` directory intact when you copy the folder
directly. If you received this through a git repository instead — where a nested
repository cannot be stored — the three commits are preserved in
`refundsplit-history.bundle`.

```bash
git clone refundsplit-history.bundle refundsplit-restored
cd refundsplit-restored
git log --oneline          # 834da14, 9aa6cbc, 6191efc
git branch -a              # main and fix/ENG-4172-penny-loss
```

The restored clone is byte-identical to the original, including both branches
and all three commit messages. The demo's time travel (`git checkout main`,
`git checkout fix/ENG-4172-penny-loss~1`) works from it exactly as the runbook
describes.
