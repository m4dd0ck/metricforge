# MetricForge: agent rules

Instructions for coding agents working in this repo; a human reader wants `README.md`. Read
`README.md` for how it works, `RUNBOOK.md` for procedures, `TASK.md` for what is open. The
owner's global agent rules apply on top of these.

- Public repo: no personal data, account numbers, keys or third-party names in any file.
- Real-time commits only; never backdate or set `GIT_AUTHOR_DATE`. No AI attribution.
- Release steps: `RUNBOOK.md` §Release. Two
  repos pin this one by tag: a release that leaves their pins behind is unfinished.
- SQL is built from validated definitions, never from user strings; a raw filter is parsed by
  SQLGlot before it is used. Keep it that way.
