# Production Tutorial

In this tutorial, we will guide you through the setup process for using PostgreSQL logical replication as a change data 
capture (CDC) source to stream documents to Kafka using the `go-pq-cdc-kafka` tool.

## Database Settings

Before setting up logical replication, you need to configure several PostgreSQL settings. Add or update the following 
parameters in your PostgreSQL configuration file (`postgresql.conf`):

* `wal_level=logical`:
  * Explanation: This setting enables logical replication by generating a log of changes in a format that allows them 
to be replicated to other systems, including non-PostgreSQL systems like Kafka.
* `max_wal_senders=5`:
  * Explanation: This parameter sets the maximum number of concurrent connections that can be used for sending WAL 
(Write-Ahead Logging) data to replicas. Each replication slot or subscription uses one WAL sender, so ensure this number
is sufficient for your replication needs.
* `max_replication_slots=5`:
  * Explanation: This setting specifies the maximum number of replication slots that PostgreSQL can use. Replication 
slots ensure that the server retains WAL files until they have been processed by all subscribers. For logical 
replication, each slot corresponds to a replication source.

 After modifying these settings, restart your PostgreSQL server to apply the changes.

## Create User 
For production use, it's recommended to use predefined replication slots and publications to minimize the permissions 
required by the CDC user. This section outlines the steps to set up a superuser to create the publication and slot, and
a dedicated user with minimal permissions for CDC operations.


- **Create a publication** that specifies which tables and changes to replicate with SUPERUSER:
```sql
CREATE PUBLICATION cdc_publication FOR TABLE users WITH (publish = 'INSERT,DELETE,UPDATE');
```

- **Create replication slot** for the CDC process with SUPERUSER:
```sql
SELECT * FROM pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
```

- Ensure the table is configured to capture necessary columns for updates and deletions. Choose `FULL` or `DEFAULT` 
based on your replication needs:
```sql
ALTER TABLE users REPLICA IDENTITY FULL;
```

- Create a user with minimal permissions needed for CDC operations:
```sql
CREATE USER es_cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_pass';
```

## Configuration
You can Check Configs detailed explanations [here](../README.md/#configuration)
You only need to configure the following fields to use the `go-pq-cdc-kafka` application:
```go
cfg := config.Config{
    CDC: cdcconfig.Config{
      Host:      "127.0.0.1",
      Username:  "cdc_user",
      Password:  "cdc_pass",
      Database:  "cdc_db",
      DebugMode: false,
      Publication: publication.Config{
        Name: "cdc_publication",
      },
      Slot: slot.Config{
        Name:                        "cdc_slot",
        SlotActivityCheckerInterval: 3000,
      },
      Metric: cdcconfig.MetricConfig{
        Port: 8081,
      },
    },
	Kafka: config.Kafka{
		TableTopicMapping:           map[string]string{"public.users": "users.0"},
		Brokers:                     []string{"localhost:19092"},
		ProducerBatchTickerDuration: time.Millisecond * 200,
	},
}
```

## Handler

The `go-pq-cdc-kafka` library supports handling `insert`, `delete`, and `update` messages (if you need other message types, feel free to open issue). <br> 
Here is an example handler function:

```go
func Handler(msg *cdc.Message) []gokafka.Message {
	slog.Info("change captured", "message", msg)
	if msg.Type.IsUpdate() || msg.Type.IsInsert() {
		newData, _ := json.Marshal(msg.NewData)

		return []gokafka.Message{
			{
				Headers: nil,
				Key:     []byte(strconv.Itoa(int(msg.NewData["id"].(int32)))),
				Value:   newData,
			},
		}
	}

	if msg.Type.IsDelete() {
		oldData, _ := json.Marshal(msg.OldData)

		return []gokafka.Message{
			{
				Headers: nil,
				Key:     []byte(strconv.Itoa(int(msg.OldData["id"].(int32)))),
				Value:   oldData,
			},
		}
	}

	return []gokafka.Message{}
}
```
 
## Deploy Strategy

The go-pq-cdc operates in passive/active modes for PostgreSQL change data capture (CDC). Here's how it ensures
availability:

* **Active Mode:** When the PostgreSQL replication slot (slot.name) is active, go-pq-cdc continuously monitors changes
  and streams them to downstream systems as configured.
* **Passive Mode:** If the PostgreSQL replication slot becomes inactive (detected via slot.slotActivityCheckerInterval),
  go-pq-cdc automatically captures the slot again and resumes data capturing. Other deployments also monitor slot
  activity,
  and when detected as inactive, they initiate data capturing.

Deploy go-pq-cdc with **`maximum two instances per cluster`** to ensure one active deployment and another ready to take over 
if the active slot becomes inactive. This setup provides redundancy and helps maintain continuous data capture without 
interruption.
