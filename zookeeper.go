package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type zookeeperCollector struct {
	upIndicator *prometheus.Desc
	metrics     map[string]zookeeperMetric
}
type zookeeperMetric struct {
	desc          *prometheus.Desc
	extract       func(string) float64
	extractLabels func(s string) []string
	valType       prometheus.ValueType
}


func parseFloatOrZero(s string) float64 {
	res, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Warningf("Failed to parse to float64: %s", err)
		return 0.0
	}
	return res
}

func NewZookeeperCollector() *zookeeperCollector {
	variableLabels := []string{"host"}
	log.Info("NewZookeeperCollector",GlobalExporterConfig.Labels)

	return &zookeeperCollector{

		upIndicator: prometheus.NewDesc("zk_up", "Exporter successful", variableLabels, GlobalExporterConfig.Labels),
		metrics: map[string]zookeeperMetric{
			"zk_avg_latency": {
				desc:    prometheus.NewDesc("zk_avg_latency", "Average latency of requests", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_max_latency": {
				desc:    prometheus.NewDesc("zk_max_latency", "Maximum seen latency of requests", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_min_latency": {
				desc:    prometheus.NewDesc("zk_min_latency", "Minimum seen latency of requests", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_packets_received": {
				desc:    prometheus.NewDesc("zk_packets_received", "Number of packets received", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.CounterValue,
			},
			"zk_packets_sent": {
				desc:    prometheus.NewDesc("zk_packets_sent", "Number of packets sent", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.CounterValue,
			},
			"zk_num_alive_connections": {
				desc:    prometheus.NewDesc("zk_num_alive_connections", "Number of active connections", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_outstanding_requests": {
				desc:    prometheus.NewDesc("zk_outstanding_requests", "Number of outstanding requests", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_server_state": {
				desc: prometheus.NewDesc("zk_server_state", "Server state (leader/follower)", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 {
					if s == "leader" {
						return 1
					} else {
						return 0
					}
				},
				valType: prometheus.UntypedValue,
			},
			"zk_znode_count": {
				desc:    prometheus.NewDesc("zk_znode_count", "Number of znodes", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_watch_count": {
				desc:    prometheus.NewDesc("zk_watch_count", "Number of watches", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_ephemerals_count": {
				desc:    prometheus.NewDesc("zk_ephemerals_count", "Number of ephemeral nodes", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_approximate_data_size": {
				desc:    prometheus.NewDesc("zk_approximate_data_size", "Approximate size of data set", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_open_file_descriptor_count": {
				desc:    prometheus.NewDesc("zk_open_file_descriptor_count", "Number of open file descriptors", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_max_file_descriptor_count": {
				desc:    prometheus.NewDesc("zk_max_file_descriptor_count", "Maximum number of open file descriptors", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.CounterValue,
			},
			"zk_followers": {
				desc:    prometheus.NewDesc("zk_followers", "Number of followers", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_synced_followers": {
				desc:    prometheus.NewDesc("zk_synced_followers", "Number of followers in sync", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_pending_syncs": {
				desc:    prometheus.NewDesc("zk_pending_syncs", "Number of followers with syncronizations pending", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 { return parseFloatOrZero(s) },
				valType: prometheus.GaugeValue,
			},
			"zk_fsync_threshold_exceed_count": {
				desc: prometheus.NewDesc("zk_fsync_threshold_exceed_count", "zk_fsync_threshold_exceed_count", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 {
					return parseFloatOrZero(s)
				},
				valType: prometheus.GaugeValue,
			},
			"zk_max_proposal_size": {
				desc: prometheus.NewDesc("zk_max_proposal_size", "zk_max_proposal_size", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 {
					return parseFloatOrZero(s)
				},
				valType: prometheus.GaugeValue,
			},
			"zk_min_proposal_size": {
				desc: prometheus.NewDesc("zk_min_proposal_size", "zk_min_proposal_size", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 {
					return parseFloatOrZero(s)
				},
				valType: prometheus.GaugeValue,
			},
			"zk_last_proposal_size": {
				desc: prometheus.NewDesc("zk_last_proposal_size", "zk_last_proposal_size", variableLabels, GlobalExporterConfig.Labels),
				extract: func(s string) float64 {
					return parseFloatOrZero(s)
				},
				valType: prometheus.GaugeValue,
			},
		},
	}
}

func (c *zookeeperCollector) Describe(ch chan<- *prometheus.Desc) {
	log.Debugf("Sending %d metrics descriptions", len(c.metrics))
	for _, i := range c.metrics {
		ch <- i.desc
	}
}

func (c *zookeeperCollector) ScrapeZookeeper(ch chan<- prometheus.Metric, z ZookeeperConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Info("Fetching metrics from Zookeeper")
	data, ok := sendZkCommand(z, "mntr")

	if !ok {
		log.Error("Failed to fetch metrics")
		ch <- prometheus.MustNewConstMetric(c.upIndicator, prometheus.GaugeValue, 0, z.Name)
		return
	}

	data = strings.TrimSpace(data)
	status := 1.0
	for _, line := range strings.Split(data, "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			log.WithFields(log.Fields{"data": line}).Warn("Unexpected format of returned data, expected tab-separated key/value.")
			status = 0
			continue
		}
		label, value := parts[0], parts[1]
		metric, ok := c.metrics[label]
		if ok {
			log.Debugf("Sending metric %s=%s", label, value)
			ch <- prometheus.MustNewConstMetric(metric.desc, metric.valType, metric.extract(value), z.Name)
		} else {
			log.Warning(fmt.Sprintf("%s not config", label))
		}
	}
	ch <- prometheus.MustNewConstMetric(c.upIndicator, prometheus.GaugeValue, status, z.Name)
	log.Print(fmt.Sprintf("Scrape Success %s ", z.Name))
}

func (c *zookeeperCollector) Collect(ch chan<- prometheus.Metric) {
	var wg sync.WaitGroup
	for _, zc := range GlobalExporterConfig.ZookeeperConfigs {
		wg.Add(1)
		go c.ScrapeZookeeper(ch, zc, &wg)
	}
	wg.Wait()
}

const (
	timeoutSeconds = 5
)

func sendZkCommand(z ZookeeperConfig, fourLetterWord string) (string, bool) {
	log.Debugf("Connecting to Zookeeper at %s", z.Address)

	conn, err := net.Dial("tcp", z.Address)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Unable to open connection to Zookeeper")
		return "", false
	}
	defer conn.Close()

	err = conn.SetDeadline(time.Now().Add(timeoutSeconds * time.Second))
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Failed to set timeout on Zookeeper connection")
		return "", false
	}

	log.WithFields(log.Fields{"command": fourLetterWord}).Debug("Sending four letter word")
	_, err = conn.Write([]byte(fourLetterWord))
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Error sending command to Zookeeper")
		return "", false
	}
	scanner := bufio.NewScanner(conn)

	buffer := bytes.Buffer{}
	for scanner.Scan() {
		buffer.WriteString(scanner.Text() + "\n")
	}
	if err = scanner.Err(); err != nil {
		log.WithFields(log.Fields{"error": err}).Error("Error parsing response from Zookeeper")
		return "", false
	}
	log.Debug("Successfully retrieved reply")

	return buffer.String(), true
}
