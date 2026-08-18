# Contrail agent guidance

Read `.agent/runtime.md` and `.agent/rules.md` before changing code. The rules
define the package layers, public compatibility boundaries, and required
evidence.

Use the repository-local skills when their trigger matches:

- `.agent/skills/maintain-python-architecture/SKILL.md` for Python modules,
  imports, packaging, typing, tests, or structural refactors.
- `.agent/skills/protect-public-contracts/SKILL.md` for runpacks, JSON output,
  CLI behavior, schemas, or release compatibility.

Preserve unrelated worktree changes. Add dependencies only with explicit user
approval. After the final edit, run the narrowest applicable commands from
`.agent/runtime.md`, then `~/.agent/bin/agent-verify --profile fast`.
