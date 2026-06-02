# AGENTS.md

## ROLE AND BOUNDARIES

* Codex performs scoped, reviewable, branch-shaped work.
* Claude Code owns main and the live deploy.
* Codex branches use codex/<task>.
* A human lead reviews each Codex branch before merge.
* Codex does not push to main.
* Codex does not touch the deploy path, secrets, or service.yaml.
* Codex writes to the separate GitLab target repo that Kineticz patches.
* Codex and Claude Code share the repo. Claude Code reads CLAUDE.md. Codex reads this file. Avoid overlap.

## STOP-BEFORE-IRREVERSIBLE CHECKPOINTS

Codex may commit on its own codex/<task> branch after these commands pass:

```bash
go build ./...
go test -race ./...
```

Stop and report before these actions. Wait for human approval before you proceed.

* Any push
* Any GitLab write, including file create, file update, branch create, or merge request
* Any merge to main
* Any secret access
* Any deploy

At each checkpoint:

* Propose the exact action.
* Show the diff or command.
* State the risk.
* Wait for human approval.
* Do not chain past the checkpoint.

Read before write:

* Run cat, head, or an equivalent read command before editing a file.
* Inspect the current file content before proposing a patch.

## SECRETS DISCIPLINE

A secret leaked once on this project. Treat secret handling as a release blocker.

* Never print secret values.
* Never pipe secrets through commands that may echo them.
* For a secret check, compare length, presence, or leading characters.
* Never expose the full value.
* Never write secrets into code, commits, logs, docs, tests, screenshots, or output.
* Stop before any secret access and ask for human approval.
* Codex does not run secret writes.
* When Codex scopes a secret write for the human, propose this form:

```bash
printf '%s' "<value>" | gcloud secrets versions add <name> --data-file=-
```

* Never use echo -n for secret writes. Shell behavior varies and can cause trailing-newline auth failures.

## WRITING RULES

These rules apply to all prose Codex produces, including code comments, docs, commit messages, error strings, logs, and CLI output.

* No throat-clearing openers.
    * Avoid: "Here's the thing"
    * Avoid: "It's worth noting"
    * Avoid: "Let's dive in"
    * Avoid: "At the end of the day"
* No contrast frame that says one thing is false, then states another. State the correct fact.
* No em dashes. Use periods or commas.
* No adverbs.
    * Avoid -ly words.
    * Avoid: "really"
    * Avoid: "just"
    * Avoid: "simply"
    * Avoid: "actually"
    * Avoid: "basically"
    * Avoid: "essentially"
    * Avoid: "fundamentally"
* No false agency for inanimate objects.
    * Avoid: "the pipeline leverages"
    * Avoid: "the system enables"
    * Name the actor.
* No vague claims.
    * Avoid: "the implications are significant"
    * Name the specific cost, risk, failure mode, or behavior.
* No rule-of-three lists unless the domain has three items.
* Use active voice.
* Use specific nouns.
* Vary sentence length.

## COMMIT MESSAGES

* Use imperative mood.
* Keep the subject under 50 characters.
* Use no adverbs.
* Use no jargon.
* Make one change per commit.
* Keep each commit bisectable by concern.

Examples:

* Add contract fetch guard
* Validate patch payload
* Fix audit span error

### GATE

* The project pre-commit gate rejects blocked commits with exit code 2.
* Treat exit code 2 as a blocked commit, not a tool failure.
* Do not bypass the gate.
* Fix the reported issue, then run the commit again.

## GO STYLE

Kineticz is a Go project.

* Check every error.
* Wrap errors with context.

```go
return fmt.Errorf("load contract: %w", err)
```

* Use typed IDs instead of raw strings.
* Validate every payload at the boundary.
* Keep handlers thin.
* Keep domain logic in typed packages.
* Prefer small interfaces near the caller.
* Avoid package-level mutable state unless the human lead approves it.
* Write tests for failure paths.

Before proposing a commit, run:

```bash
go build ./...
go test -race ./...
```

Report failures with the failing command, package, test name, and first useful error.

## DEPLOY

* Codex does not deploy.
* When Codex proposes a deploy step for the human, use this path:

```bash
./scripts/deploy.sh
```

* A new image requires a deploy-ts annotation bump in service.yaml to roll a revision.
