# Genie evaluation report

> **Status: template.** Fill in every `TODO` from the run sheets in `genie/runs/`. The tables in "Results" come
> from `python -m tools.score_runs`, not typed by hand.

## Question

Can business users get correct answers from Genie over this lakehouse? Which failures does a governed semantic
layer (Metric Views) remove, and which need extra instructions?

## Setup

| | |
|---|---|
| Questions | 25 fixed questions (`genie/benchmarks.csv`): 6 KPI, 5 breakdown, 5 returns, 5 ambiguous, 4 join-sensitive |
| Split | 15 dev (used for tuning), 10 holdout (never inspected before the final run) |
| Ground truth | SQL on the gold tables, written independently of the metric views; expected values in `genie/expected_answers.csv` |
| Trap check | For 14 questions, the most plausible wrong reading was run too and gives a different answer (`tests/test_benchmarks.py`) |
| Scoring | Built-in Genie benchmarks (exact result-set match), then a manual review of every "Bad" |
| Run A | Genie agent on the 5 gold tables, no instructions |
| Run B | Genie agent on the 2 metric views, no instructions |
| Run C | Run B + fixes motivated by dev failures only |
| Date run | TODO |

## Results

TODO: paste the output of `python -m tools.score_runs`.

| Run | Split | Strict | Reviewed |
|---|---|---|---|
| A | dev | | |
| A | holdout | | |
| B | dev | | |
| B | holdout | | |
| C | dev | | |
| C | holdout | | |

**Strict** = Genie's benchmark verdict. **Reviewed** = also counts answers that were correct but scored "Bad"
because of rounding or an extra column (`scoring_artifact`).

## What the semantic layer fixed by itself (A → B)

TODO: questions that failed in A and passed in B, grouped by failure type. Expected candidates: grain (B02, B03),
join path (B22, B23), metric definition (B19, B21). Write what actually happened.

| Question | Run A failure | Why it passed in B |
|---|---|---|
| TODO | | |

## Failures found on dev in run B and how they were fixed (B → C)

One row per fixed failure. Keep Genie's SQL short: just the part that was wrong.

| Question | Failure type | What Genie did | Fix applied | Run C |
|---|---|---|---|---|
| TODO | | | | |

## Did the fixes generalise? (holdout)

TODO: holdout accuracy B vs C. Did a fix for one dev question also fix a holdout question of the same type?
Did anything get worse?

## Remaining failures

TODO: what still fails in run C and why it wasn't fixed (e.g. would need overfitting to one question).

## Limitations

- **Small sample.** 25 questions: one question is 4 percentage points. Treat differences of one or two questions as noise.
- **Exact matching.** The benchmark compares result sets exactly. Correct answers with different rounding or an
  extra column score "Bad", hence the manual review and the two accuracy columns.
- **Non-determinism.** Genie can answer the same question differently on reruns. TODO: note whether any question
  flipped between two runs of the same configuration.
- **No conversation context.** Benchmark questions run as new conversations. Follow-up questions aren't tested.
- **Synthetic data** with clean semantics; real catalogues have messier names and more columns.
