FROM scratch
ARG KAFKA_REVISION
ARG CORE_REVISION
ARG SOURCE_STATE
LABEL org.opencontainers.image.source="https://github.com/Trendyol/go-pq-cdc-kafka" \
      org.opencontainers.image.revision="${KAFKA_REVISION}" \
      org.opencontainers.image.version="${KAFKA_REVISION}" \
      io.trendyol.go-pq-cdc.core.revision="${CORE_REVISION}" \
      io.trendyol.go-pq-cdc.source-state="${SOURCE_STATE}"
COPY --chmod=0555 --chown=65532:65532 runner /runner
USER 65532:65532
ENTRYPOINT ["/runner"]
