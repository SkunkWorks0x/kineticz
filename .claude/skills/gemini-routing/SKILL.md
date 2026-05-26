---
name: gemini-routing
description: Use when integrating Gemini 3.5 Flash, defining tool calls, or working on the diagnose or propose stages
---

# gemini-routing

## When to use
Working on Gemini 3.5 Flash integration, tool calls, or the diagnose/propose stages.

## Rules
- Use Gemini 3.5 Flash at runtime. No Claude, no GPT, no local models.
- Build through Google Cloud Agent Builder or the Gemini Enterprise Agent Platform SDK.
- Structure tool definitions so Gemini calls Dynatrace and Elastic in parallel within a single model turn.
- Feed tool call results back into Gemini for patch generation.
- Include a system instruction on every call grounding the model in current pipeline context: schema name, failure type, recent Elastic history.
- Log token usage per call for cost tracking.
- Validate that the candidate patch parses as a unified diff before advancing to the Arize gate.
