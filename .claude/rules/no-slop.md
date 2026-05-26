# No-Slop Writing Rules

Every word Claude Code writes in this project — code comments, docs, READMEs, error strings, CLI output, commit messages, PR descriptions — must pass these checks. No exceptions for "quick" tasks.

## Banned Phrases (Delete on Sight)

### Throat-Clearing Openers
- "Here's the thing"
- "It's worth noting"
- "Let's dive in / dive into / deep dive"
- "Let that sink in"
- "In today's [anything]"
- "When it comes to"
- "At the end of the day"
- "This matters because"
- "The key takeaway is"
- "Think about it"
- "What if I told you"
- "Here's what I mean"
- "Here's why this matters"
- "Consider this"

### Emphasis Crutches
- "Game-changer"
- "Paradigm shift"
- "Cutting-edge"
- "Best-in-class"
- "World-class"
- "Next-level"
- "First-class"
- "Groundbreaking"
- "Transformative"
- "Revolutionary"

### Business Jargon
- "Leverage" (as verb meaning "use")
- "Navigate" (for challenges)
- "Unpack" (before analysis)
- "Lean into"
- "Double down"
- "Move the needle"
- "Low-hanging fruit"
- "Ecosystem" (unless literally about software ecosystems)
- "Landscape" (unless literally about terrain)
- "North star"
- "Stakeholder" (say who you mean)

### Adverbs (Kill All)
- All -ly words
- "Really", "just", "simply", "actually", "basically"
- "Essentially", "fundamentally", "incredibly", "remarkably"
- "Genuinely", "honestly", "straightforward"
- "Certainly", "Moreover", "Additionally", "Furthermore"
- "Notably", "Importantly", "Interestingly"

### Meta-Commentary
- "As mentioned above/below/earlier"
- "It goes without saying"
- "Needless to say"
- "The rest of this [document/section]..."
- Any sentence that describes the structure of the text itself

### Vague Declaratives
- "The implications are significant"
- "The reasons are structural"
- "This is important"
- "There are several factors"
Name the specific implication, reason, or factor. Or delete the sentence.

## Banned Structures

### "Not X, It's Y" Contrast
WRONG: "This isn't a bug. It's a design decision."
RIGHT: "We designed it this way because [reason]."

### Dramatic Fragmentation
WRONG: "Speed. Precision. Control. That's what Kineticz delivers."
RIGHT: "Kineticz processes 10K events/sec with sub-millisecond ordering guarantees."

### Rhetorical Questions as Openers
WRONG: "What if your data pipeline could heal itself?"
RIGHT: "Kineticz restarts failed stages from the last checkpoint."

### False Agency
WRONG: "The pipeline leverages MongoDB's change streams to enable real-time sync."
RIGHT: "Kineticz tails MongoDB change streams and writes ordered events to the target."

### Rule of Three
WRONG: "Fast, reliable, and scalable."
RIGHT: "Processes 10K events/sec with zero data loss." (Specific beats tripled adjectives.)

### Narrator-from-a-Distance
WRONG: "Nobody designed distributed systems to be this simple."
RIGHT: "You configure three fields and Kineticz handles sequencing, retries, and delivery confirmation."

### Em Dashes
WRONG: "The orchestrator — which runs as a single binary — handles all routing."
RIGHT: "The orchestrator runs as a single binary and handles all routing."

## Self-Check (Run Before Returning Any Prose)

1. Does any sentence start with a banned opener? Cut to the point.
2. Any "not X, it's Y" contrast? State Y.
3. Any adverb? Delete it. If the sentence collapses, it was saying nothing.
4. Any -ly word? Kill it.
5. Three consecutive sentences match length? Break one.
6. Paragraph ends with a punchy one-liner? Vary it.
7. Em dash anywhere? Rewrite with period or comma.
8. Vague declarative? Name the specific thing or delete.
9. Inanimate object doing a human action? Name the real actor.
10. "Every", "always", "never" doing vague work? Be specific or cut.

## Code Comments Follow the Same Rules

WRONG: `// This essentially leverages the worker pool to efficiently process batches`
RIGHT: `// Fan out batch items across the worker pool. Each worker acks on completion.`

WRONG: `// Here's where the magic happens`
RIGHT: `// Sequence events by CorrelationToken, then write to MongoDB in order`

WRONG: `// TODO: Basically need to handle edge cases here`
RIGHT: `// TODO: Handle duplicate CorrelationTokens from upstream retry storms`

## Commit Messages

- Imperative mood: "Add retry logic" not "Added retry logic"
- Under 50 chars
- No adverbs, no throat-clearing, no jargon
- Body (if needed): what and why, not how

WRONG: `Essentially refactored the pipeline to be more efficient`
RIGHT: `Batch event writes to reduce MongoDB round-trips`

## Error Messages

- State what failed, what was expected, what to do
- No apologies, no softening

WRONG: `Unfortunately, we were unable to process your request. Please try again later.`
RIGHT: `Write failed: MongoDB returned timeout after 5s. Retry with backoff or check cluster health.`
