package main

import (
	"fmt"
	"goRedis/config"
	"goRedis/lib/logger"
	"goRedis/lib/utils"
	"goRedis/resp/server"
	"goRedis/tcp"
	"os"
)

var banner = `goRedis prepare to start`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     true,
	AppendFilename: "appendonly.aof",
	MaxClients:     1000,
	RunID:          utils.RandString(40),
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	fmt.Println(banner)
	// 设置日志的输出位置和名字
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "goRedis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	// 设置配置文件
	configFilename := os.Getenv("CONFIG")
	if configFilename == "" {
		if fileExists("resp.conf") {
			config.SetupConfig("resp.conf")
		} else {
			fmt.Println("goRedis start with default properties")
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFilename)
	}
	// 开启监听
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, server.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
