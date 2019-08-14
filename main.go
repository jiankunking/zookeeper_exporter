package main

import (
	"flag"
	"github.com/prometheus/common/version"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var (
	logLevel   log.Level = log.InfoLevel
	configFile           = flag.String("config-file", "", "exporter config yaml")
)

func main() {
	log.SetLevel(logLevel)
	flag.Parse()
	if *configFile == "" {
		log.Error("pls set config file")
		return
	}
	if err := LoadConfigFromFile(*configFile); err != nil {
		log.Error(err)
		return
	}
	log.Info("Starting zookeeper_exporter")
	prometheus.MustRegister(NewZookeeperCollector())
	prometheus.MustRegister(version.NewCollector("zookeeper_exporter"))

	go serveMetrics()

	exitChannel := make(chan os.Signal)
	signal.Notify(exitChannel, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	exitSignal := <-exitChannel
	log.WithFields(log.Fields{"signal": exitSignal}).Infof("Caught %s signal, exiting", exitSignal)
}

func serveMetrics() {
	log.Infof("Starting metric http endpoint on %s", GlobalExporterConfig.ListenAddress)
	http.Handle(GlobalExporterConfig.MetricsPath, prometheus.Handler())
	http.HandleFunc("/", rootHandler)
	log.Fatal(http.ListenAndServe(GlobalExporterConfig.ListenAddress, nil))
}
func rootHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<html>
		<head><title>Zookeeper Exporter</title></head>
		<body>
		<h1>Zookeeper Exporter</h1>
		<p><a href="` + GlobalExporterConfig.MetricsPath + `">Metrics</a></p>
		</body>
		</html>`))
}
