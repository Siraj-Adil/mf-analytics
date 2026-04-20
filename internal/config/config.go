package config

import (
        "os"
        "strconv"
)

// Config holds all application configuration.
type Config struct {
        Port       string
        DBPath     string
        LogLevel   string
        MFAPIBase  string

        // Rate limit configuration (matches mfapi.in constraints)
        RateLimitPerSecond int
        RateLimitPerMinute int
        RateLimitPerHour   int

        // Backfill configuration
        BackfillWorkers int

        // Known AMC target names (used for scheme discovery)
        TargetAMCs        []string
        TargetCategories  []string
        TargetSubTypes       []string
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
        return &Config{
                Port:      getEnv("PORT", "9000"),
                DBPath:    getEnv("DB_PATH", "mf_analytics.db"),
                LogLevel:  getEnv("LOG_LEVEL", "info"),
                MFAPIBase: getEnv("MFAPI_BASE", "https://api.mfapi.in"),

                // Respect mfapi.in rate limits with safety margins
                RateLimitPerSecond: getEnvInt("RATE_LIMIT_PER_SECOND", 2),
                RateLimitPerMinute: getEnvInt("RATE_LIMIT_PER_MINUTE", 50),
                RateLimitPerHour:   getEnvInt("RATE_LIMIT_PER_HOUR", 300),

                // Conservative worker count to respect rate limits
                BackfillWorkers: getEnvInt("BACKFILL_WORKERS", 1),

                // Target AMCs per assignment specification
                TargetAMCs: []string{
                        "ICICI Prudential",
                        "HDFC",
                        "Axis",
                        "SBI",
                        "Kotak",
                },

                // Target categories per assignment specification
                TargetCategories: []string{
                        "Mid Cap",
                        "Small Cap",
                },

                TargetSubTypes: []string{
                        "Direct", 
                        "Growth",
                },
        }
}

func getEnv(key, defaultVal string) string {
        if val := os.Getenv(key); val != "" {
                return val
        }
        return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
        if val := os.Getenv(key); val != "" {
                if n, err := strconv.Atoi(val); err == nil {
                        return n
                }
        }
        return defaultVal
}
