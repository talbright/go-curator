package main

import (
	"log"
	"os"
	"time"

	"github.com/talbright/go-curator"
)

var logger *log.Logger
var zkLogger *log.Logger

func init() {
	logger = log.New(os.Stdout, "[MAIN] ", log.Ldate|log.Ltime)
	zkLogger = log.New(os.Stdout, "[ZK] ", log.Ldate|log.Ltime)
}

func main() {
	logger.Print("starting go-curator")
	settings := &curator.Settings{
		ZkLogger:         zkLogger,
		ZkServers:        []string{"127.0.0.1:2181"},
		ZkSessionTimeout: 20 * time.Second,
	}
	client := curator.NewClient()
	plugins := make([]curator.Plugin, 0)
	plugins = append(plugins, &curator.EventSpewPlugin{})
	cur := curator.NewCurator(client, settings, plugins)
	cur.Start()
	select {}
}
