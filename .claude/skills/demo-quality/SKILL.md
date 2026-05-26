---
name: demo-quality
description: Use when writing README, documentation, CLI output, error messages, or preparing demo artifacts
---

# demo-quality

## When to use
Writing README, documentation, CLI output, error messages, or preparing demo artifacts.

## Judging criteria (equal weight)
Technological Implementation, Design, Potential Impact, Quality of Idea.

## Rules
- Treat every user-facing string as demo copy. Judges read the repo, watch the video, run the hosted project.
- README structure: problem statement (2 sentences), Mermaid architecture diagram, quickstart (5 commands or fewer), demo walkthrough, partner integration details.
- CLI output: structured, parseable, color-coded by stage. Show pipeline stage, correlation token, elapsed time on each line.
- Error messages: state what failed, what was expected, what to do next. No apologies.
- Place an open-source license file (MIT or Apache-2.0) at repo root. GitHub About section must detect it.
- Demo video target: 3 minutes. Show the agent detecting a real schema drift, diagnosing it, proposing a patch, passing the Arize gate, applying via GitLab, and writing the audit entry. End-to-end autonomous loop.
