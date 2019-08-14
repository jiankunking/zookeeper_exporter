package main

import (
	"encoding/json"
	"errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"strings"
)

type ExporterConfig struct {
	ListenAddress    string            `json:"listen_address" yaml:"listen_address"`
	MetricsPath      string            `json:"metrics_path" yaml:"metrics_path"`
	ZookeeperConfigs []ZookeeperConfig `json:"zookeeper_configs" yaml:"zookeeper_configs"`
	Labels           prometheus.Labels `json:"labels" yaml:"labels"`
}

type ZookeeperConfig struct {
	Name           string   `json:"name" yaml:"name"`
	Address        string   `json:"address" yaml:"address"`
	MonitorCommand []string `json:"monitor_command" yaml:"monitor_command"`
}

var GlobalExporterConfig *ExporterConfig

func init() {
	GlobalExporterConfig = &ExporterConfig{}
}

// Load ConfigFile from file, Only Support Json file or yaml file
func LoadConfigFromFile(filename string) error {
	reader, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer reader.Close()
	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	if strings.HasSuffix(filename, ".json") {
		err = json.Unmarshal(buf, GlobalExporterConfig)
	} else if strings.HasSuffix(filename, ".yaml") || strings.HasSuffix(filename, ".yml") {
		err = yaml.Unmarshal(buf, GlobalExporterConfig)
	} else {
		return errors.New("config file must be json or yaml")
	}

	log.Info("%+v", GlobalExporterConfig)

	if err != nil {
		return err
	}
	return nil
}
