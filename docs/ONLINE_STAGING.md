# Online staging runner and commit-pinned OCI archive

## Status and authority

This is a runtime/build lane for the existing
`NewConnector(ctx, cfg, Handler, ...Option)` API. The runner supplies a real,
non-nil transformation Handler; it does not use the builtin-envelope convenience
or an enqueue/response callback as a durable receipt.

The core API required by this source is not present in published v1.14.16.
Local qualification uses the actual source trees in an external Go workspace.
Remote CI must bind two committed full SHAs; a published module download or an
archive of the starting HEADs does not include the approved uncommitted changes.

**Core source pin:** `.ci/online-core-ref` binds the reviewed core commit
`4305e394798a16fd9389c1bb2074e29de59c986a`. The workflow checks out that exact
commit and checks Kafka against its captured event SHA; both appear in the image
version and artifact manifest. CI rejects the earlier implementation placeholder.
Only parent may stage, commit, normally push the two approved new build branches
and dispatch the registered `Build` workflow. No PR, main merge, force push,
release tag, package/registry publication or deployment is part of this lane.
Local builds alone do not establish successful remote artifact acceptance.

## Runtime configuration

Run `/runner --version` without loading configuration or connecting anywhere.
Commit-pinned builds report both full SHAs, actual Go version, platform and
`sourceState`. A plain development build reports `uncommitted`; explicitly
allowed local dirty-tree archive builds report `uncommitted-local`, never
`committed`.

Normal capture requires `--config /run/config/runtime.yaml`. YAML is strict:
unknown/duplicate fields, multiple documents and malformed values fail closed.
Configuration/CA files are bounded regular files; credentials are runtime-only.
The runner requires online initial capture, protocol 1, acks-all, no automatic
Kafka topic creation, no oversized-message skipping and disabled debug mode.
Core still admits only the direct-primary/FULL/immutable-integer-key/frozen-schema
profile. The operator must pre-provision the `cdc_online` metadata relations,
explicit publication (business tables plus `cdc_online.watermarks`), business-table
`REPLICA IDENTITY FULL` and destination topics. The sample disables publication
creation. It explicitly authorizes the connector to create its fresh named slot
on the first run (`slot.createIfNotExists: true`); that slot must not already be
an unbound slot or belong to another connector. Resume retains the same managed
job and slot rather than recreating them. Slot creation is a runtime source-side
operation, not a build operation. The build itself makes no source connection,
migration or staging operation.

Example non-secret configuration (replace names and endpoints at runtime):

```yaml
keyColumn: id
healthAddress: 0.0.0.0:8081
startupTimeout: 30s
connector:
  cdc:
    host: primary.example.internal
    port: 5432
    username: runtime_user
    database: source_database
    metric:
      port: 8080
    publication:
      name: staging_publication
      createIfNotExists: false
      operations: [insert, update, delete, truncate]
      tables:
        - schema: public
          name: items
          replicaIdentity: FULL
    slot:
      name: staging_slot
      protoVersion: 1
      createIfNotExists: true
    snapshot:
      enabled: true
      mode: initial
      strategy: online
      chunkSize: 100
      tables:
        - schema: public
          name: items
  kafka:
    brokers: [broker.example.internal:9092]
    requiredAcks: -1
    balancer: Hash
    allowAutoTopicCreation: false
    skipOversizedMessages: false
    tableTopicMapping:
      public.items: staging.items
    online:
      maxAttempts: 3
      closeTimeout: 5s
```

`keyColumn` is explicit: every captured table must use that actual integer-family
primary-key column. The runner accepts the real decoded int16/int32/int64 values;
it does not guess a key or stringify unsupported types. A missing/unsupported key
or marshal failure records a bounded private transformation-failure flag and
returns one explicitly invalid record. Protected existing delivery rejects that
record before any write/ACK; the process closes its owned resources and exits
nonzero with `transform`. It never silently filters or panics on a marshal error.

### Secret and trust precedence

For passwords, precedence is explicit environment value (including empty), then
Secret file, then mounted YAML. An empty explicit value does not fall back.
Password files may have one terminating newline. Supported overrides:

| Variable | Meaning |
| --- | --- |
| `CDC_PG_PASSWORD`, `CDC_PG_PASSWORD_FILE` | source password/value or mounted file |
| `CDC_PG_USERNAME` | explicit source username override |
| `CDC_KAFKA_SCRAM_PASSWORD`, `CDC_KAFKA_SCRAM_PASSWORD_FILE` | Kafka SCRAM password |
| `CDC_KAFKA_SCRAM_USERNAME` | Kafka SCRAM username |
| `CDC_KAFKA_ROOT_CA_FILE` | overrides `kafkaRootCAFile` runtime path |
| `CDC_KAFKA_INTER_CA_FILE` | overrides `kafkaInterCAFile` runtime path |

PostgreSQL TLS uses the actual drivers' `PGSSLMODE` and `PGSSLROOTCERT` environment
variables, not an invented source Config field. Choose explicit `disable` for the
trusted plaintext fixture profile, or `verify-full` with a readable mounted PEM
CA file in `PGSSLROOTCERT`. Other modes are not supported by this runner profile.
Scratch contains no system trust store, so roots cannot be assumed present.

Kafka supports plaintext **without** SASL/CA fields, or `secureConnection: true`
with nonempty SCRAM-SHA512 username/password and a valid runtime root PEM CA.
Optional intermediate PEM can be supplied. TLS-only, plaintext SASL, Kafka mTLS,
OAuth and other SASL modes are not advertised. Credentials and CA files are not
copied into the image. Make mounted configuration/Secret files readable by image
UID/GID `65532:65532` (for example an operator-approved filesystem group); do not
bake credentials or broaden file permissions merely to bypass admission.

## Payload, metadata and delivery

The auditable payload has version `1`, schema/table, `operation`, configured
`keyColumn`, exact integer `key`, actual `before` and `after` images. Existing
`SNAPSHOT` input becomes `READ`; INSERT/UPDATE/DELETE retain their meanings, with
DELETE's real before image and null after image. Keys use the existing qualified,
length-delimited source key helper. Topic mapping remains runtime configuration.
The content-type identifies this custom staging payload, not the builtin online
envelope.

Protected constructor delivery augments stable `cdc-*` event/order/output-index
headers and clones owned images and record buffers. Sender observation time is
not deterministic replay content and is intentionally excluded from this payload.
Only the owned synchronous RequireAll completion permits source progress; the
Handler itself is transformation, not a receipt. Delivery remains at least once:
partial writes and retries can duplicate records. No exactly-once, downstream
completeness, HA or power-loss certification is implied.

## Lifecycle and health

`/readyz` and `/livez` use the configurable runner health port, distinct from core
metrics. Readiness is false until actual `WaitUntilReady` succeeds, and also
requires running/not-Done/not-failed/not-stopping state. Core's fixed `/status`
response is not a readiness oracle. Responses contain bounded state/category and
readiness, never raw backend errors or configuration.

SIGTERM and SIGINT revoke readiness and release the owned capture/writer and
health listener. Cooperative cleanup exits zero; startup, delivery, transform,
health or incomplete cleanup failures exit nonzero with static categories.
Startup timeout is bounded to five minutes; configured writer close timeout is
bounded to thirty seconds (unset uses the existing five-second default). Health
shutdown has its own two-second listener budget. SIGHUP is not a reload or
supported graceful-stop API.

The backend logger discards raw messages/arguments and emits only constant
component/severity fields for warnings/errors. Debug/info payload logging is
not enabled. Do not infer that raw backend diagnostic text is safe to publish.

## Build and artifact verification

The SAME registered `.github/workflows/build.yml` / `Build` workflow is reused.
Only `workflow_dispatch` on
`refs/heads/build/online-staging-kafka-20261113` selects the archive job. Main push,
PR and ordinary/manual-main behavior remain intact. The inherited security job
remains unchanged and naturally skips via `needs: build` on the isolated route;
that skip is **not** reported as a security pass. The staging job has its own
serial source/runner/static/process gates, contents-read-only permissions, no
inherited secrets, no cache/persisted checkout credentials and no publication.

Kafka is checked out at the event SHA; core at the validated single 40-hex pin.
Both HEADs and clean trees are verified. The workspace is generated under runner
temporary storage and `go list -m` must resolve the actual core checkout. Modules
remain readonly; no tidy or invented remote semver is used.

Immutable action versions are checkout v7.0.1, setup-go v7.1.0, upload-artifact
v7.0.2 and setup-buildx v4.4.1 at the already verified full commits in the workflow.
Their immutable action metadata uses Node24; the hosted Ubuntu runner must meet
its minimum runner requirement (2.327.1 or later). Go1.26.10's Linux amd64 SDK
checksum and Buildx v0.39.0 binary checksum are independently verified; the
BuildKit linux/amd64 image is pinned to an actually resolved immutable digest.
The manifest records actual tool/builder and hosted image versions.

Core integration selects `POSTGRES_VERSION`; Kafka integration selects
`POSTGRES_TEST_IMAGE`. They are explicitly distinct in CI. The prior constructor
handoff's purported core-PG17 log actually used PG16; parent supplied the missing
corrected PG17 proof with `POSTGRES_VERSION=17-alpine`, actual 170011 server and
exit zero. Do not treat the old mislabeled log as PG17 proof or rerun that already
corrected source profile merely because this runner lane was added.

The driver compiles CGO-disabled linux/amd64 with trimpath and both SHA bindings.
Only `/runner` and the minimal Dockerfile enter the packaging context. The image
is scratch, non-root and has executable `/runner` entrypoint and both source
revision labels. No source tree, .git, SDK, runtime config, CA or credential file
is copied. A builder may supply only its numeric Buildah provenance label and the
standard constant PATH; arbitrary labels/environment are rejected.

The bundle contains an OCI archive, schema-1 build manifest, version JSON and
SHA256SUMS. Verification covers descriptor sizes/digests, rootfs digests, source
labels, platform, user/entrypoint, allowlisted layer paths, extracted ELF/binary
checksum and actual version. Unreferenced blobs, traversal, unexpected files,
corruption and wrong bindings fail before upload. The outer tar SHA256 is distinct
from OCI content descriptor digests and from GitHub's upload transport digest.

Local dirty-tree qualification (not a committed/remote source binding):

```sh
PYTHONDONTWRITEBYTECODE=1 python3 packaging/online-staging/build.py \
  --core ../go-pq-cdc --kafka . \
  --core-sha "$CORE_STARTING_HEAD" --kafka-sha "$KAFKA_STARTING_HEAD" \
  --engine podman --podman-connection podman-machine-default-root \
  --local-uncommitted --output /absolute/external/qualification-directory
```

Use the explicit verified forwarded Docker-compatible socket for local
Testcontainers; do not assume `/var/run/docker.sock` exists. Do not initialize,
resize or change machines/connections or clean foreign containers. Local Podman
build/run/export uses owned fixtures only. The CI route uses the Docker-container
Buildx driver and explicit OCI exporter, never a registry push.

An OCI-capable importer is required. Verify SHA256SUMS and the manifest before
importing. Podman `load --input <archive>` is the supported local importer; do not
assume `docker load` compatibility. Kubernetes/containerd import and deployment
require the platform operator's separately approved runtime procedure and are
not performed by this build lane. No cluster credentials are available here.

Final remote acceptance still requires parent-authorized commits/pin substitution,
ordinary approved branch pushes, dispatch of Build at that branch, successful run
with matching head SHA, downloadable bundle and independent revalidation of the
archive/manifest/checksums. A local archive or placeholder pin is not remote
artifact success.
