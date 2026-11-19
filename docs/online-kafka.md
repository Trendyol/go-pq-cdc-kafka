# Online Kafka delivery

The existing `NewConnector(ctx, cfg, Handler, ...Option)` selects this path internally for
`snapshot.strategy: online`. The exact constructor, Handler and Option types stay unchanged.
Consistent/legacy mode keeps its original batching and response-handler semantics.

## What "delivered" means

Your existing non-nil `Handler` synchronously transforms one owned `Message` into Kafka records.
Returning records is **not** a durable receipt. The connector validates the entire fanout before
writing, then calls `kafka.Writer.WriteMessages` once per output using owned synchronous
`RequiredAcks: RequireAll` (`-1`), `Async: false`, `BatchSize: 1`.

- Every broker write succeeds ⇒ the internal core delivery callback may certify completion.
- Any rejection/write failure ⇒ core does not acknowledge that source event.
- Context cancellation or a deadline is an **unconfirmed outcome**: the record may still have been
  appended, so it is counted as `Unknown`, never as written, and the source does not advance. The
  event is redelivered later, which is why consumers must tolerate duplicates.

Delivery is **at least once**. This is not an exactly-once consumer guarantee, and no throughput,
power-loss or HA claim is made by the tests in this repository.

## Required configuration

| Setting | Requirement |
| :--- | :--- |
| `snapshot.strategy` | `online` (core gate: primary-only, protocol 1, FULL images, immutable integer key, frozen schema) |
| `kafka.requiredAcks` | `-1`; an unset value defaults to `-1` **only** on this path (legacy default stays `1`) |
| `kafka.balancer` | `Hash`, `ReferenceHash`, `CRC32Balancer` or `Murmur2Balancer` (key preserving) |
| `kafka.tableTopicMapping` | one explicit topic per captured table; no implicit or empty destination |
| `kafka.allowAutoTopicCreation` | `false`; provision topics with the intended partition count |
| `kafka.skipOversizedMessages` | `false`; an oversized envelope stops delivery |
| `kafka.maxMessageBytes` | upper bound of one output including mandatory headers, checked before any write |
| `kafka.producerBatchSize` | maximum outputs per Handler call (default `2000`) |
| `kafka.producerBatchBytes` | maximum aggregate output wire bytes per Handler call (default `1mb`) |
| `kafka.online.maxAttempts` | finite retry bound (default `3`, maximum `10`) |
| `kafka.online.closeTimeout` | bound for cooperative shutdown (default `5s`) |

The online constructor validates all of this before it opens a connection and returns
`ErrOnlineAdmission` with the failing facts. It owns the Kafka transport, so `Close()` releases the
writer and its idle connections; caller contexts are never cancelled.

## Existing Handler contract

`Message.Type` is `SNAPSHOT` for READ and `INSERT`/`UPDATE`/`DELETE` for CDC. Table namespace/name
remain qualified. `NewData` is the actual complete after image (nil for DELETE); `OldData` is the
actual FULL before image for UPDATE/DELETE (nil for INSERT/READ). Integer-family types and exact JSON
numbers are retained. Input maps/nested JSON/arrays are copied before your callback; output key/value
and headers are copied before the durable writes, without mutating caller-owned records. Concurrent
mutation while returning/copying records is still the caller's data race, not supported ownership.

For CDC, `LSN` is the actual decoded row WALStart, `TransactionID` is real source XID, and `EventTime`
is XLogData.ServerTime (source sender clock, **not** transaction commit time). READ uses the dedicated
HIGH boundary for `LSN`, zero/unavailable XID, and zero/unavailable `EventTime` (`IsZero()` is true).
No synthetic wall clock, epoch, transaction ID or before image is invented. Sender observation time
can differ on replay. Core `Event` excludes only that field from its JSON content digest, but the
existing Kafka `Message.EventTime` remains available to your transformation. Do not use this volatile
clock to define deterministic replay payloads or identity.

Payload/key/topic/custom headers remain your transformation. Empty topic defaults to the configured
mapped topic; explicit valid topics are preserved. Require a nonempty key and value; raw tombstones,
empty filtering results, invalid topics/headers, any caller `cdc-*` reserved header, or fanout/size
limits fail closed **before any write**. `WithLogger` and `WithPrometheusMetrics` are staged safely and
forwarded; `WithResponseHandler` is unsupported online even when passed nil. Legacy filtering,
response receipts and batching are not silently reinterpreted as durable delivery.

Mandatory headers on every output:

| Header | Value |
| :--- | :--- |
| `cdc-metadata-version` | `1` (independent of payload format) |
| `cdc-event-id` | verbatim protected core event ID |
| `cdc-order-end-lsn` | canonical protected order boundary `X/Y` |
| `cdc-order-phase` | decimal phase, `1` READ / `0` CDC |
| `cdc-order-ordinal` | decimal source order ordinal |
| `cdc-output-index` | zero-based Handler output ordinal |
| `cdc-output-id` | stable `OnlineOutputID(eventID, index)` |

All outputs must succeed before source progress. Later failure can leave earlier outputs durable,
so retries can duplicate those outputs. Your Handler must preserve deterministic output order,
content, key and destinations across retries. The connector cannot certify arbitrary nondeterminism
or external callback effects as exactly once. Consumers must retain identity/order tombstones and
apply an appropriate deduplication/order policy for the custom payload. Delivery counters count
records, not whole completed source events.

## Optional builtin envelope convenience

`NewOnlineConnector` remains available without a custom Handler. Only this convenience uses the
versioned JSON envelope (`OnlineEnvelopeVersion`, currently `1`):

| Field | Meaning |
| :--- | :--- |
| `version` | envelope version; a consumer that does not know it must stop |
| `id` | stable event identity; identical for a redelivered event |
| `schema`, `table` | qualified source table |
| `key` | the immutable integer primary key, as a JSON integer |
| `operation` | `READ`, `INSERT`, `UPDATE`, `DELETE` |
| `commitLsn` | source commit LSN (`0/0` for a synthetic `READ`) |
| `order.endLsn`, `order.phase`, `order.ordinal` | delivery version; phase `1` is `READ`, phase `0` is CDC |
| `image` | complete row for `READ`/`INSERT`/`UPDATE`, `null` for `DELETE` |

Builtin headers: `content-type`, `cdc-envelope-version`, plus the mandatory metadata headers above.

The Kafka key is `len(schema):schema` + `len(table):table` + decimal key, which keeps exactly one
partition per row for a key-preserving balancer even when table names contain dots, digits or
underscores.

LSNs are canonical PostgreSQL `X/Y` text. Row keys and JSON values keep their exact decimal digits, so
a consumer must decode with `json.Decoder.UseNumber` (`DecodeOnlineEnvelope` does) instead of routing
numbers through `float64`; `9007199254740993` stays exact.

`DELETE` carries identity and order but no image, so a consumer can retain a tombstone. A `READ`
(phase 1) taken before a delete must never resurrect the row: apply records with an order guard on
`(endLsn, phase, ordinal)` and deduplicate by `id`. `integration_test/online_kafka_test.go` contains a
worked projection that does exactly this and asserts it against the source table.

## Lifecycle and failure behaviour

| Element | Behaviour |
| :--- | :--- |
| `Start` | starts core capture once; a call after `Close` is ignored |
| `WaitUntilReady` | capture readiness; returns `ErrOnlineConnectorClosed` after `Close` |
| `WaitUntilSnapshotComplete` | READ phase delivered |
| `Done` / `Err` | terminal signal and reason (`context.Canceled` for a cooperative close) |
| `Close` | one overall `kafka.online.closeTimeout` budget: cancels its own context, aborts owned broker I/O, then joins capture and releases the writer/transport |
| `Stats` | core capture counters plus `Written` / `Failed` / `Unknown` / `Rejected` delivery counters |
| `Health` | `State` (`new`, `running`, `closing`, `closed`, `failed`), `Err`, `Running`; never running after `Close` |

A broker outage stops the run instead of dropping events, and the managed job checkpoint
(`cdc_online.jobs.source_lsn`) stays where the last acknowledged event left it.

### Shutdown semantics

`Close` uses ONE budget. It first aborts the broker I/O this connector owns. Cancelling a request
context is not enough: kafka-go detaches the waiting caller (`async.await`) but its connection worker
keeps using the socket under that request's own `WriteTimeout` deadline, and `CloseIdleConnections`
only releases sockets that are already idle. The writer therefore dials through an owned boundary that
tracks every connection from successful dial through final close, including the API-version
handshake and an in-flight `Produce`, and shutdown **actively closes those sockets** and refuses new
dials. That is what ends a withheld `Produce` immediately instead of a long write timeout later, and
the writer only reports a clean close after the owned sockets are gone; otherwise it returns an
incomplete-teardown error that the connector surfaces as `ErrOnlineShutdownIncomplete` with state
`failed`.

Evidence for this is protocol-level: the regression completes a real API-version negotiation and
metadata exchange, observes a `Produce` request with `acks=-1`, withholds its response, and then
requires the server to see the client close that socket while `Close` returns inside the budget. It is
an observed withheld response, not a power-loss or protocol-level lost-response certification.

If capture/writer cleanup still overruns the budget, `Close` returns anyway and the connector reports
`ErrOnlineShutdownIncomplete` through `Err()`/`Health().Err`; the cleanup continues in the background
and is never presented as a clean close. A blocking user handler cannot be force-killed by this
package: cancellation is cooperative, and an unacknowledged write stays unacknowledged at the source.

## Sink continuity and rebootstrap

The source job fingerprint covers capture configuration and schema, **not** the Kafka cluster or the
topic mapping. Therefore:

- Reusing a source job (same slot/managed job) against a different cluster, a recreated topic, an
  emptied topic or a new partition count does **not** rebuild the history that was already
  acknowledged. Resume starts from the source checkpoint; it is not a re-snapshot of the sink.
- A source job assumes a stable destination: same cluster, same topic per table, same key/envelope
  protocol, and retained log plus consumer state that still covers the source checkpoint. Kafka
  retention expiry of those records is data loss for a consumer that rebuilds from the log; the
  source cannot detect it.
- A new or truncated sink requires an explicitly fresh capture/rebootstrap and a reconciliation of the
  destination against the source. Recreating the topic while resuming the same job is not a valid
  re-initialization.
- Readiness and source acknowledgement do not certify downstream consumer completeness. They only
  state that capture started and that the broker acknowledged each delivered record.
- No automatic sink binding enforcement is implemented: the connector validates configuration, not
  cross-system identity, and it cannot know what a consumer has applied.

## Documented limits

- Raw event-topic compaction is not replay safe: compaction may keep only the newest record per key, so
  a consumer rebuilding state from the log must not rely on `compact` topics. Retention by time/size is
  the supported horizon.
- Changing a topic's partition count can move a key to another partition. Provision the final partition
  count up front; re-partitioning is outside this contract.
- Integration evidence is produced against a single-node Apache Kafka broker (`RF=1`, `minISR=1`) plus
  one PostgreSQL instance. It proves broker protocol acknowledgement and source gating, not HA,
  power-loss durability or throughput.
- A cancelled write is an unknown outcome by design; no test in this repository claims otherwise. A
  caller callback that blocks cannot be force-killed by this package: cancellation is cooperative.
- The socket-level test withholds a `Produce` response and requires the socket to be closed; it does
  not claim durability, power-loss safety or an interrupted-checkpoint duplicate-replay guarantee.
- The server's `confirmed_flush_lsn` follows PostgreSQL's standby status-update protocol (the reader's
  safe boundary), so it is not a per-event oracle. The per-event ACK-gated truth is the managed job
  checkpoint, which the tests assert stays frozen while a write is unacknowledged.
- This source needs a matching core containing the online factory and metadata. Published v1.14.16
  does not contain it. Local builds use the actual source workspace; the staging OCI build remains
  pending exact parent-owned core/Kafka commits and CI qualification, not a package release.
