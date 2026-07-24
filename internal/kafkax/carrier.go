// Package kafkax wraps the franz-go client with the produce and consume
// behaviour SentinelFlow relies on: synchronous delivery acknowledgement,
// manual offset commits, and W3C trace context carried in record headers.
package kafkax

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
)

// RecordCarrier adapts a Kafka record's headers to OpenTelemetry's
// TextMapCarrier so the standard propagators can inject and extract trace
// context without Kafka-specific code.
//
// Kafka headers allow duplicate keys; Set replaces an existing key rather than
// appending so a re-published record cannot carry two traceparent values.
type RecordCarrier struct {
	record *kgo.Record
}

// NewRecordCarrier wraps rec. The record must not be nil.
func NewRecordCarrier(rec *kgo.Record) RecordCarrier {
	return RecordCarrier{record: rec}
}

var _ propagation.TextMapCarrier = RecordCarrier{}

// Get returns the first header value for key, or "" when absent.
func (c RecordCarrier) Get(key string) string {
	for _, h := range c.record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set writes key, replacing any existing header with the same key.
func (c RecordCarrier) Set(key, value string) {
	for i := range c.record.Headers {
		if c.record.Headers[i].Key == key {
			c.record.Headers[i].Value = []byte(value)
			return
		}
	}
	c.record.Headers = append(c.record.Headers, kgo.RecordHeader{
		Key:   key,
		Value: []byte(value),
	})
}

// Keys lists every header key present on the record.
func (c RecordCarrier) Keys() []string {
	keys := make([]string, 0, len(c.record.Headers))
	for _, h := range c.record.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}
