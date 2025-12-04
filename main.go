package main

import (
	"data-check-all/model"
	"data-check-all/service"
	"fmt"
	"go.yaml.in/yaml/v4"
	"log"
	"os"
)

type Config struct {
	Clickhouses []model.ClickHouseConfig `yaml:"clickhouse"` // Note: lowercase to match YAML key
	Tidbs       []model.TiDBConfig       `yaml:"tidb"`
	Tikvs       []model.TiKVConfig       `yaml:"tikv"`
	Ess         []model.ESConfig         `yaml:"es"`
}

func main() {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatal(err)
	}

	// Example: Print parsed config
	fmt.Printf("ClickHouse: %+v\n", config.Clickhouses)
	fmt.Printf("TiDB: %+v\n", config.Tidbs)
	fmt.Printf("TiKV: %+v\n", config.Tikvs)
	fmt.Printf("ES: %+v\n", config.Ess)

	service.TestES(config.Ess)
	service.TestTiKV(config.Tikvs)
	service.TestClickHouse(config.Clickhouses)
	service.TestTiDB(config.Tidbs)
}
