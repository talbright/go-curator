package main

import (
	"log"
	"os"
	"time"

	"github.com/talbright/go-curator"
	"github.com/talbright/go-curator/plugin"
)

var logger *log.Logger
var zkLogger *log.Logger

func init() {
	logger = log.New(os.Stdout, "[MAIN] ", log.Ldate|log.Ltime)
	zkLogger = log.New(os.Stdout, "[ZK] ", log.Ldate|log.Ltime)
}

func main() {
	logger.Print("starting go-curator")
	appId := os.Getenv("EXAPP_APP_ID")
	if appId == "" {
		appId = "app1"
	}
	settings := &curator.Settings{
		ZkLogger:         zkLogger,
		ZkServers:        []string{"127.0.0.1:2181"},
		ZkSessionTimeout: 20 * time.Second,
		RootPath:         "/services/curator",
	}
	client := curator.NewClient()
	plugins := make([]curator.Plugin, 0)
	plugins = append(plugins, &plugin.EventSpew{})
	plugins = append(plugins, &plugin.Leader{})
	plugins = append(plugins, &plugin.Member{ID: appId})
	plugins = append(plugins, &plugin.Discovery{})
	cur := curator.NewCurator(client, settings, plugins)
	cur.Start()
	select {}
}
