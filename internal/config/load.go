package config

import (
	"os"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

func read(filename string) ([]byte, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read file %s", filename)
	}
	return data, nil
}

func unmarshal(content []byte) (*Config, error) {
	cfg := &Config{}
	if err := yaml.Unmarshal(content, cfg); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal config file: %s", content)
	}
	return cfg, nil
}

// Load reads and parses a YAML configuration file.
func Load(filename string) (*Config, error) {
	content, err := read(filename)
	if err != nil {
		return nil, err
	}
	return unmarshal(content)
}
