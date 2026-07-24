package kafkax

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// kgoLogger forwards franz-go's internal logs into the service's slog handler
// so that client-level warnings (broker retries, rebalances, coordinator loss)
// appear in the same JSON stream as everything else instead of on raw stderr.
type kgoLogger struct {
	log *slog.Logger
}

func newKgoLogger(log *slog.Logger) kgo.Logger {
	return &kgoLogger{log: log.With(slog.String("component", "franz-go"))}
}

// Level tells franz-go how verbose to be. Info is the useful ceiling; debug
// emits a line per fetch, which drowns the demo output.
func (l *kgoLogger) Level() kgo.LogLevel {
	switch {
	case l.log.Enabled(context.Background(), slog.LevelDebug):
		return kgo.LogLevelDebug
	case l.log.Enabled(context.Background(), slog.LevelInfo):
		return kgo.LogLevelInfo
	case l.log.Enabled(context.Background(), slog.LevelWarn):
		return kgo.LogLevelWarn
	default:
		return kgo.LogLevelError
	}
}

func (l *kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	l.log.Log(context.Background(), toSlogLevel(level), msg, keyvals...)
}

func toSlogLevel(level kgo.LogLevel) slog.Level {
	switch level {
	case kgo.LogLevelDebug:
		return slog.LevelDebug
	case kgo.LogLevelWarn:
		return slog.LevelWarn
	case kgo.LogLevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
