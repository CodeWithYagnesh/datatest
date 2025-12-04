package model

import ("time")

type TestKeyValue struct {
	Key   string    `json:"key"`
	Value string    `json:"value"`
	Ts    time.Time `json:"timestamp"`
}

type TiKVConfig struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	SSLClientCRT string `yaml:"ssl_client_crt"`
	SSLClientKey string `yaml:"ssl_client_key"`
	SSLCACRT     string `yaml:"ssl_ca_crt"`
	Prefix       string `yaml:"prefix"`
}
