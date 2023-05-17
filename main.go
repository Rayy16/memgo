package main

import (
	"fmt"
	"memgo/config"
	"memgo/logger"
	"memgo/redis/RESP/handler"
	"memgo/tcp"
	utils "memgo/utils/rand_string"
	"os"
)

var banner = `
    ___     ___     _______          
   / |\ \ / || \   / _____/____  
  / /  \ / / | |  / / ___/ ___ \
 / /    | /  | | / /__/ / /__/ /
/_/     |/    \_\\_____/\_____/
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6379,
	AppendOnly:     true,
	AppendFsync:    "everysec",
	AppendFilename: "0517test.aof",
	MaxClients:     5000,
	RunID:          utils.RandString(40),
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	print(banner)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "memgo",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}

	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, handler.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
