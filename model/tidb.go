package model

type TiDBConfig struct {
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	Database     string `yaml:"database"`
	TableName    string `yaml:"table_name"`
	SSLClientCRT string `yaml:"ssl_client_crt"`
	SSLClientKey string `yaml:"ssl_client_key"`
	SSLCACRT     string `yaml:"ssl_ca_crt"`
	TLS          bool   `yaml:"tls"`
}
