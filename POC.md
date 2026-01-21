# POC Approach

## Goal

Evaluate whether scraped Kadaster query examples can improve SPARQL generation/execution via few-shot prompting against a single target endpoint.

## Intended Integration

This POC feeds into a Model Context Protocol (MCP) server/tooling so that an agent (e.g. via `gemini-cli`) can:

- Retrieve curated few-shot examples derived from the scraped query set.
- Execute predefined “safe” SPARQL queries.
- Execute generated SPARQL against the single chosen endpoint for evaluation.

## Steps

1. Scrape ~365 queries from the Kadaster Labs website and use them to evaluate/choose proper few-shot examples.
2. Create a small set of predefined “safe” SPARQL queries to fall back on and to use as control cases.
3. Use one SPARQL endpoint as the execution target for the few-shot evaluation.
4. Evaluate results (end goal).
