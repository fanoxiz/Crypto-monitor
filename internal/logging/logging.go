package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

func Configure(service string) error {
	level := slog.LevelInfo
	rawLevel := strings.ToUpper(strings.TrimSpace(os.Getenv("LOG_LEVEL")))

	if rawLevel != "" {
		if err := level.UnmarshalText([]byte(rawLevel)); err != nil {
			return fmt.Errorf("invalid LOG_LEVEL %q: %w", rawLevel, err)
		}
	}

	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.TimeKey {
				return slog.Time(slog.TimeKey, attr.Value.Time().UTC())
			}

			return attr
		},
	})
	slog.SetDefault(slog.New(handler).With("service", service))

	slog.Debug("logging_configured", "component", "main", "configured_level", level.String())

	return nil
}
