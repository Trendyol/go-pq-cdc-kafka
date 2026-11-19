package mapper

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc/change"
	"github.com/Trendyol/go-pq-cdc/pq/timescaledb"
	gokafka "github.com/segmentio/kafka-go"
)

// Value formats a Mapping can name.
const (
	ValueRow      = "row"
	ValueEnvelope = "envelope"
)

var configHeaders = map[string]func() Step{
	OperationHeaderKey: OperationHeader,
	TableHeaderKey:     TableHeader,
	LSNHeaderKey:       LSNHeader,
	XIDHeaderKey:       XIDHeader,
}

// Option changes what FromConfig builds.
type Option func(o *options)

type options struct {
	steps    map[string][]Step
	fallback cdc.Handler
}

// WithSteps adds steps written in code to a table that has a mapping. They run after the
// mapping's column steps (operations, include, exclude, mask, rename, insertFields) and
// before its Kafka steps (key, headers, topics, tombstone, value).
func WithSteps(table string, steps ...Step) Option {
	return func(o *options) {
		name := change.QualifiedName(table)
		o.steps[name] = append(o.steps[name], steps...)
	}
}

// WithFallback handles the tables that have no mapping. It defaults to DefaultMapper.
// Passing the handler a connector uses today lets tables move to config one at a time.
func WithFallback(handler cdc.Handler) Option {
	return func(o *options) {
		o.fallback = handler
	}
}

// FromConfig builds a handler from kafka.Mappings: one mapper per table, built from
// change.Config and the Kafka fields in a fixed order, and the fallback for other tables.
// It reports every problem in the mappings at once, so a typo fails at startup.
func FromConfig(kafka config.Kafka, opts ...Option) (cdc.Handler, error) {
	o := &options{steps: map[string][]Step{}, fallback: DefaultMapper}
	for _, opt := range opts {
		opt(o)
	}

	topics := make(map[string]struct{}, len(kafka.TableTopicMapping))
	for table := range kafka.TableTopicMapping {
		topics[change.QualifiedName(table)] = struct{}{}
	}

	mappers := make(map[string]cdc.Handler, len(kafka.Mappings))
	seen := make(map[string]string, len(kafka.Mappings))
	var errs []error
	for _, table := range slices.Sorted(maps.Keys(kafka.Mappings)) {
		name := change.QualifiedName(table)
		if other, dup := seen[name]; dup {
			errs = append(errs, fmt.Errorf("mapping %s: %q and %q name the same table", name, other, table))
			continue
		}
		seen[name] = table
		if _, ok := topics[name]; len(topics) > 0 && !ok {
			errs = append(errs, fmt.Errorf("mapping %s: table is not in tableTopicMapping, so the connector never hands it to the mapper", name))
			continue
		}

		steps, err := mappingSteps(kafka.Mappings[table], o.steps[name])
		if err != nil {
			errs = append(errs, fmt.Errorf("mapping %s: %w", name, err))
			continue
		}
		mappers[name] = New(steps...)
	}
	for _, table := range slices.Sorted(maps.Keys(o.steps)) {
		if _, ok := seen[table]; !ok {
			errs = append(errs, fmt.Errorf("WithSteps %s: table has no mapping", table))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	return func(msg *cdc.Message) []gokafka.Message {
		return route(mappers, o.fallback, msg)
	}, nil
}

func route(mappers map[string]cdc.Handler, fallback cdc.Handler, msg *cdc.Message) []gokafka.Message {
	name := msg.TableNamespace + "." + msg.TableName
	if m, ok := mappers[name]; ok {
		return m(msg)
	}
	// TimescaleDB chunks use their hypertable's mapping, as they use its topic.
	if parent, ok := timescaledb.HyperTables.Load(name); ok {
		if m, ok := mappers[parent.(string)]; ok {
			return m(msg)
		}
	}
	return fallback(msg)
}

// mappingSteps turns a mapping into steps in a fixed order: the column steps, the custom steps,
// then key, headers, topic, operation topics, tombstone and value.
func mappingSteps(m config.Mapping, custom []Step) ([]Step, error) {
	var errs []error

	// Steps comes from the embedded change.Config: the column steps only.
	columnSteps, err := m.Steps()
	if err != nil {
		errs = append(errs, err)
	}
	steps := []Step{Apply(columnSteps...)}
	steps = append(steps, custom...)

	if len(m.Key) > 0 {
		for _, col := range m.Key {
			if keyColumnRemoved(m.Config, col) {
				errs = append(errs, fmt.Errorf("key column %q is removed by include, exclude or rename", col))
			}
		}
		steps = append(steps, KeyFromColumns(m.Key...))
	}

	for _, name := range m.Headers {
		header, ok := configHeaders[strings.ToLower(name)]
		if !ok {
			errs = append(errs, fmt.Errorf("headers: unknown header %q, want one of operation, table, lsn, xid", name))
			continue
		}
		steps = append(steps, header())
	}

	if m.Topic != "" {
		steps = append(steps, Topic(m.Topic))
	}
	for _, op := range slices.Sorted(maps.Keys(m.OperationTopics)) {
		normalized := change.Op(strings.ToUpper(string(op)))
		switch {
		case !isKnownOp(normalized):
			errs = append(errs, fmt.Errorf("operationTopics: unknown operation %q", op))
		case m.OperationTopics[op] == "":
			errs = append(errs, fmt.Errorf("operationTopics: empty topic for %s", normalized))
		default:
			steps = append(steps, When(change.IsOp(normalized), Topic(m.OperationTopics[op])))
		}
	}

	if m.TombstoneOnDelete {
		steps = append(steps, TombstoneOnDelete())
	}

	switch strings.ToLower(m.Value) {
	case "", ValueRow:
	case ValueEnvelope:
		steps = append(steps, EnvelopeValue())
	default:
		errs = append(errs, fmt.Errorf("value: unknown value %q, want %s or %s", m.Value, ValueRow, ValueEnvelope))
	}

	return steps, errors.Join(errs...)
}

func isKnownOp(op change.Op) bool {
	switch op {
	case change.Insert, change.Update, change.Delete, change.Snapshot:
		return true
	default:
		return false
	}
}

// keyColumnRemoved reports whether the column steps leave no column named col for the key to read.
func keyColumnRemoved(cfg change.Config, col string) bool {
	if _, ok := cfg.InsertFields[col]; ok {
		return false
	}
	source := col
	for from, to := range cfg.Rename {
		if to == col {
			source = from
		}
	}
	if _, renamedAway := cfg.Rename[col]; renamedAway && source == col {
		return true
	}
	if slices.Contains(cfg.Exclude, source) {
		return true
	}
	return len(cfg.Include) > 0 && !slices.Contains(cfg.Include, source)
}
