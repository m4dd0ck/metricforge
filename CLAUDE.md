# MetricForge: agent rules

Read `README.md` for how it works, `RUNBOOK.md` for procedures, `TASK.md` for what is open.
Global rules in `~/.claude/CLAUDE.md` apply.

- Public repo: no personal data, account numbers, keys or third-party names in any file.
- Real-time commits only; never backdate or set `GIT_AUTHOR_DATE`. No AI attribution.
- Release steps follow `~/developer/hq/CONVENTIONS.md`; the procedure is in `RUNBOOK.md`. Two
  repos pin this one by tag: a release that leaves their pins behind is unfinished.
- SQL is built from validated definitions, never from user strings; a raw filter is parsed by
  SQLGlot before it is used. Keep it that way.
