package main

import (
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := loadConfig(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.httpAddr != ":8080" || cfg.maxInFlight != 3 || cfg.databaseURL != "" || cfg.backoffBase != 0 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := loadConfig(env(map[string]string{
		"HTTP_ADDR": ":9000", "MAX_IN_FLIGHT": "5", "SUBMIT_TIMEOUT": "2s",
		"LEASE_DURATION": "1m", "BACKOFF_BASE": "500ms", "LOG_LEVEL": "debug",
		"DATABASE_URL": "postgres://x",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.httpAddr != ":9000" || cfg.maxInFlight != 5 || cfg.submitTimeout != 2*time.Second ||
		cfg.leaseDuration != time.Minute || cfg.backoffBase != 500*time.Millisecond || cfg.databaseURL != "postgres://x" {
		t.Errorf("unexpected config: %+v", cfg)
	}
}

func TestLoadConfigRejectsBadValues(t *testing.T) {
	for key, val := range map[string]string{
		"MAX_IN_FLIGHT": "0", "SUBMIT_TIMEOUT": "soon", "LEASE_DURATION": "-1s", "LOG_LEVEL": "loud",
	} {
		if _, err := loadConfig(env(map[string]string{key: val})); err == nil {
			t.Errorf("%s=%q accepted", key, val)
		}
	}
}
