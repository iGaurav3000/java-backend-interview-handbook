# Java Backend Interview Handbook

Interview preparation for backend engineers with 2–7 years of experience targeting product companies — written from production experience, not from a syllabus.

Most Java interview material stops where real backend work starts. It covers `HashMap` vs `Hashtable` and the SOLID acronym, then hands you a Spring Boot CRUD tutorial. That is not what gets asked when you interview for SDE-2 or SSE. What gets asked is whether you understand the machinery you use every day: what a transaction annotation actually does at the proxy boundary, what happens to your delivery guarantee during a consumer rebalance, why your connection pool is sized wrong.

This handbook covers that layer.

## Who this is for

- Backend engineers with roughly 2–7 years of experience, preparing for SDE-2 / SSE interviews at product companies.
- People who can already write the code but get caught on the follow-up question: *"okay, but what happens if two instances do that at the same time?"*

## Who this is not for

- Complete beginners. This assumes you have shipped Java services and used Spring, Kafka and a relational database in anger.
- Anyone looking for a list of 500 questions to memorize. There are plenty of those. This is about understanding mechanisms well enough to answer questions nobody prepared you for.

## Contents

### Distributed systems
- [Kafka delivery semantics and idempotency](distributed-systems/kafka-delivery-semantics.md) — what at-least-once actually costs you, where the offset commit belongs, and why exactly-once stops at the Kafka boundary

### Java internals
*In progress.* Collections under concurrency, the memory model, garbage collection, virtual threads and what they do to `synchronized`.

### Spring internals
*In progress.* Proxies and self-invocation, transaction propagation, context lifecycle.

### Low-level design
*In progress.* Worked problems with class design and the trade-offs named explicitly.

### High-level design
*In progress.* Designs with the follow-up questions interviewers actually probe.

### DSA patterns
*In progress.* Fifteen patterns, each problem with a short approach line rather than a full solution dump.

### Behavioral and manager rounds
*In progress.*

A new section lands most weeks. Watch the repo if you want them as they appear.

## How to use this

Read a section, then close it and try to explain the mechanism out loud to nobody in particular. If you stumble, you have found the gap the interviewer would have found. That is the whole method.

## About

Everything here is drawn from publicly documented technology and general patterns. No employer-internal material appears in this repository.

## Going deeper

This handbook is free and stays free.

If you want structured preparation — a full question bank across DSA, LLD, system design and behavioral rounds, or a mock interview with specific feedback on where your answer loses the room — I offer that separately: Will be adding here Topmate profile soon.

## Corrections

If something here is wrong, open an issue. Technical corrections are genuinely welcome and will be credited.

## License

Content licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Use it, share it, teach from it — just keep the attribution.
