# Config-based Kubernetes deployment

Same image, one Consul `config.json` per pipeline. Do not create a new Go
repository for each PostgreSQL → Kafka hat.

Slot is exclusive: **max 2 replicas** per `slotName`. Production publications
and slots should already exist (`createIfNotExists: false`).

## 1. Application (replicas: 0)

```yaml
kind: Application
apiVersion: core.oam.dev/v1beta1
metadata:
  name: <your-custom-name>
spec:
  components:
    - name: <your-custom-name>
      type: ty-webservice
      properties:
        image: <registry>/go-pq-cdc-kafka:<version>
        resources:
          limits:
            cpu: "2"
            memory: 2Gi
          requests:
            cpu: 300m
            memory: 512Mi
        language: go
        applicationType: 3rdparty-app
        terminationGracePeriodSeconds: 120
        traffic:
          serviceDiscoveryName: <your-custom-name>
          ports:
            - name: cdc-port
              port: 8080
              protocol: TCP
          enabled: true
        configSecret:
          fileDir: /app/config
          configKeys:
            config.json: configs
        livenessProbe:
          failureThreshold: 45
          tcpSocket:
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 5
        readinessProbe:
          failureThreshold: 45
          tcpSocket:
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 10
      traits:
        - type: scaler
          properties:
            replicas: 0
        - type: annotations
          properties:
            prometheus.io/path: /metrics
            prometheus.io/port: "8080"
            prometheus.io/scrape: "true"
```

## 2. Consul `config.json`

`slotName` and `publicationName` must be unique on the PostgreSQL cluster.
Changing `slotName` creates a new replication slot; it does not replay the old one.

```json
{
  "postgresSecretPath": "config/postgres-secret.json",
  "postgresHost": "postgres.example",
  "postgresPort": 5432,
  "postgresDatabase": "orders",
  "publicationName": "cdc_publication_orders",
  "publicationCreateIfNotExists": false,
  "slotName": "cdc_slot_orders",
  "slotCreateIfNotExists": false,
  "tables": [
    {"name": "orders", "schema": "public", "replicaIdentity": "FULL"}
  ],
  "tableTopicMapping": {
    "public.orders": "team.orders.cdc"
  },
  "keyField": "id",
  "kafkaBrokers": ["broker1:9092", "broker2:9092"],
  "kafkaSecretPath": "config/kafka-secret.json",
  "kafkaSecureConnection": true,
  "kafkaRootCAPath": "/etc/ssl/certs/root.pem",
  "kafkaInterCAPath": "/etc/ssl/certs/inter.pem"
}
```

Secret file JSON shape: `{"username":"...","password":"..."}`.

## 3. Scale

After Consul is populated, set replicas to `1` (or `2` for active/passive failover).

TCP probes use the metrics port (`cdc.metric.port`, default 8080). Keep
`initialDelaySeconds` high enough for the metrics server to bind.

Host and port are separate fields (`postgresHost` / `postgresPort`). A host
value of `postgres.example:5432` is also accepted and split.

Local overlay:

```bash
export CONFIG_YAML_PATH=./resources/config.yml
export CDC_CONSUL_CONFIG_PATH=./resources/testdata/consul-overlay.example.json
go run ./cmd/connector
```

`CONFIG_PATH` is still read if `CDC_CONSUL_CONFIG_PATH` is unset.
