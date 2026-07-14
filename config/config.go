package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig
	Postgres PostgresConfig
	Redis    RedisConfig
	Buffer   BufferConfig
	Worker   WorkerConfig
}

type ServerConfig struct {
	Host             string
	Port             int
	ReadTimeoutSecs  int
	WriteTimeoutSecs int
}

type PostgresConfig struct {
	DSN string
}

type RedisConfig struct {
	URL string
}

type BufferConfig struct {
	Capacity uint64
}

type WorkerConfig struct {
	Count         int
	MaxConcurrent int
}

// Load reads configuration from environment variables (and optional .env file).
// Env var names are SCREAMING_SNAKE_CASE versions of the dot-separated key:
// e.g. server.port → SERVER_PORT, buffer.capacity → BUFFER_CAPACITY.
func Load() (*Config, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// defaults
	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout_secs", 10)
	v.SetDefault("server.write_timeout_secs", 30)
	v.SetDefault("postgres.dsn", "postgres://helios:helios@localhost:5432/helios?sslmode=disable")
	v.SetDefault("redis.url", "redis://localhost:6379")
	v.SetDefault("buffer.capacity", 4096)
	v.SetDefault("worker.count", 8)
	v.SetDefault("worker.max_concurrent", 32)

	cfg := &Config{
		Server: ServerConfig{
			Host:             v.GetString("server.host"),
			Port:             v.GetInt("server.port"),
			ReadTimeoutSecs:  v.GetInt("server.read_timeout_secs"),
			WriteTimeoutSecs: v.GetInt("server.write_timeout_secs"),
		},
		Postgres: PostgresConfig{
			DSN: v.GetString("postgres.dsn"),
		},
		Redis: RedisConfig{
			URL: v.GetString("redis.url"),
		},
		Buffer: BufferConfig{
			Capacity: uint64(v.GetInt64("buffer.capacity")),
		},
		Worker: WorkerConfig{
			Count:         v.GetInt("worker.count"),
			MaxConcurrent: v.GetInt("worker.max_concurrent"),
		},
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	cap := c.Buffer.Capacity
	if cap == 0 || cap&(cap-1) != 0 {
		return fmt.Errorf("buffer.capacity must be a power of 2, got %d", cap)
	}
	if c.Worker.Count <= 0 {
		return fmt.Errorf("worker.count must be > 0")
	}
	return nil
}
