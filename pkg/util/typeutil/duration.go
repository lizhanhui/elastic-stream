package typeutil

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
)

// Duration is a wrapper of time.Duration for TOML and JSON.
type Duration struct {
	time.Duration
}

// NewDuration creates a Duration from time.Duration.
func NewDuration(duration time.Duration) Duration {
	return Duration{Duration: duration}
}

// MarshalJSON returns the duration as a JSON string.
func (d *Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON parses a JSON string into the duration.
func (d *Duration) UnmarshalJSON(text []byte) error {
	var v interface{}
	if err := json.Unmarshal(text, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		duration, err := time.ParseDuration(value)
		if err != nil {
			return errors.WithMessage(err, "parse from string")
		}
		d.Duration = duration
		return nil
	default:
		return errors.New("invalid duration")
	}
}

// MarshalText returns the duration as a JSON string.
func (d *Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// UnmarshalText parses a TOML string into the duration.
func (d *Duration) UnmarshalText(text []byte) error {
	duration, err := time.ParseDuration(string(text))
	if err != nil {
		return errors.WithMessage(err, "parse duration from text")
	}
	d.Duration = duration
	return nil
}
