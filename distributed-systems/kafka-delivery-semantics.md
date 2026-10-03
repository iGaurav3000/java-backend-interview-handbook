# Kafka Delivery Semantics and Idempotency

> **The question, as it actually gets asked:** *"Your consumer processes an order event and writes to the database. The pod gets killed mid-processing. What happens?"*
>
> The junior answer is "Kafka will redeliver it." That is correct and it earns nothing, because it answers the easy half. The question is really about what *you* did to make redelivery safe.

---

## The three guarantees, stated precisely

People say "at-least-once" as though it were a setting. It is not. It is an outcome of where you put the offset commit relative to the side effect.

| Guarantee | What it means | What it costs you |
|---|---|---|
| At-most-once | Every message is delivered zero or one times | You can silently lose messages |
| At-least-once | Every message is delivered one or more times | You will reprocess duplicates |
| Exactly-once | Every message has effect exactly once | Only achievable within specific boundaries — see below |

Almost every real system runs at-least-once and makes the processing idempotent. The entire design problem is the second half of that sentence, and that is where interviews go.

---

## Producer side: what `enable.idempotence` really buys

Without idempotence, a producer that times out waiting for an ack retries, and the broker may have already written the first attempt. You get a duplicate on the partition, and if `max.in.flight.requests.per.connection > 1`, retries can also reorder records.

The idempotent producer fixes both. The broker assigns each producer a **producer ID (PID)**, and the producer attaches a monotonically increasing **sequence number** per partition. The broker tracks the last sequence it accepted and silently drops anything it has already seen.

```properties
enable.idempotence=true          # default since Kafka 3.0
acks=all                         # implied by idempotence
retries=2147483647               # implied
max.in.flight.requests.per.connection=5   # must be <= 5
```

**The limitation that matters in an interview:** the PID is assigned per producer *session*. Restart the producer and it gets a new PID, so the broker has no memory of the old sequence numbers. Idempotence protects you against retries within a session. It does nothing about your service redeploying and republishing work it had already published.

Crossing that boundary requires a `transactional.id`, which is stable across restarts and lets the broker fence the previous incarnation of that producer.

---

## Consumer side: the commit is the whole game

Everything about your delivery guarantee is decided by one choice — where the offset commit sits relative to the side effect.

**Commit before processing → at-most-once.** The crash loses the message.

```java
// at-most-once: do not do this unless loss is genuinely acceptable
var records = consumer.poll(Duration.ofMillis(500));
consumer.commitSync();
for (var record : records) {
    process(record);   // crash here and this message is gone forever
}
```

**Commit after processing → at-least-once.**

```java
// at-least-once: the correct default
var records = consumer.poll(Duration.ofMillis(500));
for (var record : records) {
    process(record);
}
consumer.commitSync();   // crash before this and the batch is redelivered
```

### Auto-commit gives you neither, reliably

`enable.auto.commit=true` is the default and it is the source of a genuinely common production bug. The commit happens inside `poll()`, once `auto.commit.interval.ms` has elapsed, and it commits the offsets of records **already returned to you** — regardless of whether you finished processing them.

So you get message loss when the commit lands before processing completes, and duplicates when it doesn't. You do not get to choose which, and the behaviour changes with load.

For anything with a real side effect, set `enable.auto.commit=false` and commit explicitly.

---

## What actually happens during a rebalance

A rebalance is triggered when a consumer joins, leaves, or is declared dead. Under the classic eager protocol it is stop-the-world: every consumer gives up every partition, then the group reassigns.

The failure mode is this: you were halfway through a batch, the rebalance revoked your partitions, and you never committed. The new owner starts from the last committed offset and reprocesses everything you had already done. If `process()` wasn't idempotent, you have just double-charged someone.

Commit on revocation:

```java
consumer.subscribe(List.of("orders"), new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        consumer.commitSync(currentOffsets);   // last chance before losing them
    }
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) { }
});
```

Two configuration details worth knowing by name:

- **`partition.assignment.strategy=CooperativeStickyAssignor`** — incremental rebalancing. Consumers keep the partitions they are not losing instead of dropping everything. Available since 2.4; the sensible default for anything latency-sensitive. Kafka 4.0's new consumer group protocol (KIP-848) moves assignment to the broker and removes most of this pain, but plenty of production clusters are not there yet.

- **`max.poll.interval.ms`** (default 5 minutes) — if processing a batch takes longer than this, the broker assumes you are dead and rebalances your partitions away. Your eventual `commitSync()` then fails with `CommitFailedException`, and the work is reprocessed by someone else. This is the classic "it worked in staging" bug: a slow downstream call pushes batch processing past the interval, and the consumer group starts thrashing.

  Note that heartbeats run on a background thread, so the group thinks you are alive the whole time. `session.timeout.ms` catches a dead process; `max.poll.interval.ms` catches a slow one. Fix it by lowering `max.poll.records`, not by raising the interval to an hour.

---

## Exactly-once, and exactly where it stops

Kafka transactions give genuine exactly-once for the **consume → process → produce** pattern, entirely within Kafka. The offset commit and the output records land in the same transaction:

```java
producer.initTransactions();

while (true) {
    var records = consumer.poll(Duration.ofMillis(500));
    producer.beginTransaction();
    for (var record : records) {
        producer.send(transform(record));
    }
    producer.sendOffsetsToTransaction(offsetsOf(records), consumer.groupMetadata());
    producer.commitTransaction();
}
```

Downstream consumers must set `isolation.level=read_committed`, or they will read aborted records.

**Now the part that separates the senior answer.** This guarantee covers Kafka-to-Kafka. The moment `process()` writes to Postgres, calls a payment API, or sends an email, you are outside the transaction. Kafka cannot roll back a `POST`. There is no configuration that extends exactly-once across that boundary, and claiming otherwise in an interview is a tell.

What you do instead is make the external effect idempotent.

---

## The idempotent consumer

Pick a deduplication key that is stable across redeliveries — the event's business ID, or a producer-assigned UUID carried in a header. Not the offset, which changes if the event is ever republished.

Then write the dedup record and the business change **in the same database transaction**:

```java
@Transactional
public void handle(OrderEvent event) {
    try {
        processedEvents.insert(event.eventId());   // UNIQUE constraint on event_id
    } catch (DuplicateKeyException e) {
        return;                                    // already handled, drop it
    }
    orders.applyPayment(event.orderId(), event.amount());
}
```

The unique constraint is doing the real work. Two consumers racing on the same event: one insert wins, the other gets a constraint violation and exits. Both commit a consistent result.

What makes this correct is that the marker and the effect share one transaction. If you check a Redis set first and *then* write to the database, you have a window between the two where a crash leaves the event marked as done but not actually done — a silent data loss bug that will take you a week to find.

Housekeeping: that table grows forever, so partition it by day or run a scheduled delete beyond your maximum redelivery window.

---

## Follow-ups the interviewer has ready

Prepare for these specifically. They are where the conversation goes once you have given a good first answer.

**"What if the operation isn't naturally idempotent — incrementing a balance?"**
Store the resulting state, not the delta. `UPDATE accounts SET balance = ? WHERE id = ? AND version = ?` with optimistic locking, or keep the dedup table and make the increment conditional on the insert succeeding.

**"How do you handle a poison message?"**
Bounded retries, then route to a dead-letter topic with the original headers plus the failure reason. Retrying forever blocks the partition behind it — a single bad message stalls every subsequent message on that partition, which is the actual outage.

**"Can you process the same partition in parallel?"**
Not without giving up ordering. Partition count is your parallelism ceiling for a consumer group. If you need more throughput, add partitions, or hand records off to a worker pool keyed by entity ID so ordering holds per entity — and then you own the commit logic yourself, because an offset is only safe to commit once every record below it has completed.

**"You've increased partitions. What broke?"**
Key-to-partition mapping changed, so events for the same entity now land on a different partition and ordering is no longer preserved across the change. Partition count is effectively one-way.

---

## The shape of a senior answer

Junior: *"Kafka guarantees at-least-once, so the message gets redelivered."*

Senior: *"At-least-once, because I commit after processing. So redelivery is expected and processing has to be idempotent — I dedupe on the event ID in the same transaction as the business write, so a crash can't leave the two out of sync. Kafka transactions would give exactly-once if the output were another topic, but we're writing to Postgres, so that boundary doesn't hold and idempotency is the real mechanism."*

Same facts. The second one names the trade-off and the limit, which is what the interviewer is listening for.

---

*Found an error? Open an issue — corrections are credited.*

