---
name: hackathon-rules
description: Use for any decision about architecture, dependencies, or tooling for the hackathon submission
---

# hackathon-rules

## When to use
Any decision about architecture, dependencies, or tooling.

## Competition
- Google Cloud Rapid Agent Hackathon.
- Deadline: 2026-06-11 14:00 PDT.
- Maximum team size: 4. Solo permitted.

## Required
- Gemini 3 family (using 3.5 Flash).
- Google Cloud Agent Builder.
- At least one partner MCP server.
- Public, open-source repo with a license file at root.
- Hosted project URL. Deploy on Google Cloud (Cloud Run recommended).

## Forbidden at runtime
- Competing AI services: Anthropic (Claude API, SDKs), OpenAI, local LLMs. Claude Code remains a build-time tool only.
- Competing cloud platforms for core infrastructure.

## Submission
- Demo video, hosted URL, repo URL, text description, selected partner track.
- Pick the partner track where Kineticz has the deepest integration. Track choice sets the competition bucket.
- All code, assets, and run instructions live in the repo. Judges must be able to run it.
