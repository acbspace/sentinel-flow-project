package kafkax

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerConfig describes the consumer group membership and poll sizing.
type ConsumerConfig struct {
	Brokers        []string
	Topic          string
	Group          string
	MaxPollRecords int
	ClientID       string
	SessionTimeout time.Duration
}

// Consumer is a consumer-group member with automatic offset commits disabled.
type Consumer struct {
	client *kgo.Client
	topic  string
	group  string
	log    *slog.Logger
}

// NewConsumer builds a Kafka consumer-group client.
//
// Two options define the delivery semantics:
//
//   - DisableAutoCommit stops the client from advancing offsets on a timer, so
//     an offset is only ever committed by code that has already persisted the
//     record. This is what turns the pipeline into at-least-once instead of
//     at-most-once.
//   - BlockRebalanceOnPoll keeps a rebalance from landing between processing and
//     committing a batch. The partitions stay assigned until AllowRebalance is
//     called after the commit.
func NewConsumer(cfg ConsumerConfig, log *slog.Logger) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka consumer requires at least one broker")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka consumer requires a topic")
	}
	if cfg.Group == "" {
		return nil, errors.New("kafka consumer requires a group")
	}
	if cfg.SessionTimeout <= 0 {
		cfg.SessionTimeout = 45 * time.Second
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		// A brand new group starts at the beginning of the topic so that events
		// produced before the engine first started are not skipped.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.WithLogger(newKgoLogger(log)),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			log.Info("partitions assigned", slog.Any("partitions", assigned))
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			log.Info("partitions revoked", slog.Any("partitions", revoked))
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			// Lost means the group membership expired before a clean revoke.
			// Records already processed but not committed will be redelivered.
			log.Warn("partitions lost", slog.Any("partitions", lost))
		}),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	return &Consumer{client: client, topic: cfg.Topic, group: cfg.Group, log: log}, nil
}

// Poll fetches the next batch of records.
//
// It returns the records plus any fetch errors. Fetch errors are returned
// rather than swallowed so the caller can log them; they are usually transient
// (a broker restarting) and the client retries on the next poll.
func (c *Consumer) Poll(ctx context.Context, maxRecords int) ([]*kgo.Record, []error, bool) {
	if maxRecords <= 0 {
		maxRecords = 100
	}

	fetches := c.client.PollRecords(ctx, maxRecords)
	if fetches.IsClientClosed() {
		return nil, nil, true
	}

	var fetchErrs []error
	fetches.EachError(func(topic string, partition int32, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		fetchErrs = append(fetchErrs, fmt.Errorf("fetch %s[%d]: %w", topic, partition, err))
	})

	records := make([]*kgo.Record, 0, fetches.NumRecords())
	fetches.EachRecord(func(r *kgo.Record) {
		records = append(records, r)
	})

	return records, fetchErrs, false
}

// Commit marks records as processed.
//
// CommitRecords commits the offset *after* the highest record supplied for each
// partition, which is the offset the group resumes from. It is only called once
// the corresponding rows are durable in PostgreSQL.
func (c *Consumer) Commit(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	if err := c.client.CommitRecords(ctx, records...); err != nil {
		return fmt.Errorf("commit offsets for %s: %w", c.topic, err)
	}
	return nil
}

// AllowRebalance releases the rebalance block taken by the previous poll. It
// must be called exactly once per poll, after the batch has been committed.
func (c *Consumer) AllowRebalance() {
	c.client.AllowRebalance()
}

// Ping verifies broker connectivity; it backs the readiness probe.
func (c *Consumer) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}
	return nil
}

// Close leaves the consumer group cleanly.
//
// Leaving the group explicitly triggers an immediate rebalance instead of
// making the remaining members wait out the session timeout.
func (c *Consumer) Close() {
	c.client.Close()
}
