package celery

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	BrokerURL          string
	CelerydConcurrency int
	CeleryAcksLate     bool
	// CeleryResultSerializer   string
	// CeleryResultExchange     string
	// CeleryResultExchangeType string
	// CeleryResultPersistent   bool
	// CeleryTaskResultExpires  int
	// CeleryResultBackend string
}

var defaultConfig = Config{
	BrokerURL:          "amqp://localhost//",
	CeleryAcksLate:     true,
	CelerydConcurrency: 100,
	// CeleryResultSerializer: "json",
	// CeleryResultExchange:     "celeryresults",
	// CeleryResultExchangeType: "direct",
	// CeleryResultPersistent:   false,
	// CeleryTaskResultExpires:  -1,
	// CeleryResultBackend: "amqp",
}

func ConfigFromEnv() Config {
	return Config{
		BrokerURL:          envString("BROKER_URL", defaultConfig.BrokerURL),
		CeleryAcksLate:     envBool("CELERY_ACKS_LATE", defaultConfig.CeleryAcksLate),
		CelerydConcurrency: envInt("CELERYD_CONCURRENCY", defaultConfig.CelerydConcurrency),
	}
}

func envString(key string, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}

func envInt(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}

	i, err := strconv.ParseInt(val, 10, 0)

	if err != nil {
		panic(fmt.Sprintf("Failed to parse int from %s=%v: %v", key, val, err))
	}

	return int(i)
}

func envBool(key string, def bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return def
	}

	b, err := strconv.ParseBool(val)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse bool from %s=%v: %v", key, val, err))
	}

	return b
}
