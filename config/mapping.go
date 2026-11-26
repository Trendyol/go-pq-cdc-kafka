package config

import "github.com/Trendyol/go-pq-cdc/change"

// Mapping describes how one table's changes become Kafka messages, for mapper.FromConfig.
// The fields of change.Config (operations, include, exclude, mask, rename, insertFields)
// sit at the same level as the Kafka fields:
//
//	mappings:
//	  public.users:
//	    exclude: [password]
//	    key: [id]
//	    headers: [operation, table]
//	    operationTopics: {DELETE: users.deleted}
//	    tombstoneOnDelete: true
//	    value: row
type Mapping struct {
	// OperationTopics sends an operation's messages to another topic, such as deletes to users.deleted.
	OperationTopics map[change.Op]string `json:"operationTopics" yaml:"operationTopics" mapstructure:"operationTopics"`
	// Topic sends every message of the table to this topic instead of its tableTopicMapping topic.
	Topic string `json:"topic" yaml:"topic" mapstructure:"topic"`
	// Value is "row" (the default: the row as JSON) or "envelope" ({"op", "before", "after", "source"}).
	Value string `json:"value" yaml:"value" mapstructure:"value"`
	// Key lists the columns the key is built from, joined with ":". The names are the ones after rename.
	Key []string `json:"key" yaml:"key" mapstructure:"key"`
	// Headers lists metadata headers to add: operation, table, lsn, xid.
	Headers       []string `json:"headers" yaml:"headers" mapstructure:"headers"`
	change.Config `json:",inline" yaml:",inline" mapstructure:",squash"`
	// TombstoneOnDelete sends deletes with a nil value.
	TombstoneOnDelete bool `json:"tombstoneOnDelete" yaml:"tombstoneOnDelete" mapstructure:"tombstoneOnDelete"`
}
