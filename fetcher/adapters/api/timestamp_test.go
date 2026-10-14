package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestSourceTimeUsesMillisecondsAndUTC(t *testing.T) {
	for encoded, want := range map[string]time.Time{
		"1":               time.Date(1970, 1, 1, 0, 0, 0, 1000000, time.UTC),
		"1700000000123":   time.Date(2023, 11, 14, 22, 13, 20, 123000000, time.UTC),
		"253402300799999": time.Date(9999, 12, 31, 23, 59, 59, 999000000, time.UTC),
	} {
		t.Run(encoded, func(t *testing.T) {
			got := sourceTime(json.Number(encoded))
			if !got.Equal(want) || got.Location() != time.UTC {
				t.Fatalf("source time = %s (%s), want %s in UTC", got, got.Location(), want)
			}
		})
	}
}

func TestSourceTimeRejectsInvalidTimestamps(t *testing.T) {
	for name, encoded := range map[string]string{
		"missing":           "",
		"zero":              "0",
		"negative":          "-1",
		"positive_overflow": "9223372036854775808",
		"negative_overflow": "-9223372036854775809",
		"fractional":        "1700000000123.5",
		"exponent":          "1e3",
		"text":              "not-a-timestamp",
		"date_string":       "2023-11-14T22:13:20.123Z",
		"whitespace":        " 1700000000123 ",
	} {
		t.Run(name, func(t *testing.T) {
			if got := sourceTime(json.Number(encoded)); !got.IsZero() {
				t.Fatalf("invalid timestamp %q became %s", encoded, got)
			}
		})
	}
}

func timestampPayload(t *testing.T, fixture protocolFixture, primary, fallback string) []byte {
	t.Helper()

	var fields map[string]json.RawMessage

	if err := json.Unmarshal([]byte(fmt.Sprintf(fixture.quote, "XAUT")), &fields); err != nil {
		t.Fatal(err)
	}

	fields["ts"] = json.RawMessage(fallback)
	if fixture.exchange == "Bybit" {
		fields["cts"] = json.RawMessage(primary)
	} else {
		var items []map[string]json.RawMessage

		if err := json.Unmarshal(fields["data"], &items); err != nil {
			t.Fatal(err)
		}

		items[0]["ts"] = json.RawMessage(primary)
		data, err := json.Marshal(items)
		if err != nil {
			t.Fatal(err)
		}

		fields["data"] = data
	}

	payload, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}

	return payload
}

func TestBookDecodersHandleUnusableTimestampsAndFallback(t *testing.T) {
	decoders := map[string]func([]byte) (streamMessage, error){
		"Bybit":  decodeBybit,
		"Bitget": decodeBitget,
		"OKX":    decodeOKX,
	}

	for _, fixture := range protocolFixtures() {
		decode, exists := decoders[fixture.exchange]
		if !exists {
			continue
		}

		t.Run(fixture.exchange, func(t *testing.T) {
			for name, primary := range map[string]string{
				"null":     "null",
				"negative": "-1",
				"overflow": "9223372036854775808",
				"fraction": "1700000000123.5",
				"exponent": "1e3",
			} {
				t.Run(name, func(t *testing.T) {
					message, err := decode(timestampPayload(t, fixture, primary, "1700000000456"))
					if err != nil {
						t.Fatal(err)
					}

					wantTime := time.Time{}
					if fixture.exchange != "OKX" {
						wantTime = time.Date(2023, 11, 14, 22, 13, 20, 456000000, time.UTC)
					}

					if message.quote.Coin != "XAUT" || message.quote.Exchange != fixture.exchange || message.quote.Invalidated || !message.quote.SourceAt.Equal(wantTime) {
						t.Fatalf("wrong identity, invalidation or timestamp fallback: %#v, want source_at %s", message.quote, wantTime)
					}
				})
			}

			t.Run("both_unusable", func(t *testing.T) {
				message, err := decode(timestampPayload(t, fixture, "-1", "9223372036854775808"))
				if err != nil {
					t.Fatal(err)
				}

				if message.quote.Coin != "XAUT" || message.quote.Invalidated || !message.quote.SourceAt.IsZero() {
					t.Fatalf("unusable timestamps invented a source time or invalidation: %#v", message.quote)
				}
			})

			for name, primary := range map[string]string{
				"malformed_string": `"bad-timestamp"`,
				"boolean":          "true",
				"array":            "[]",
				"object":           "{}",
			} {
				t.Run(name, func(t *testing.T) {
					message, err := decode(timestampPayload(t, fixture, primary, "1700000000456"))
					if err == nil || errors.Is(err, errRequestRejected) || !reflect.DeepEqual(message, streamMessage{}) {
						t.Fatalf("malformed timestamp became a quote, invalidation, fallback or connection refusal: %#v, %v", message, err)
					}
				})
			}
		})
	}
}
