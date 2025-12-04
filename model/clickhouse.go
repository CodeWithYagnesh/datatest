package model

type ClickHouseConfig struct {
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	Username       string `yaml:"username"`
	Password       string `yaml:"password"`
	Database       string `yaml:"database"`
	DBTableName    string `yaml:"db_table_name"`
	LocalTableName string `yaml:"local_table_name"`
	TLS            bool   `yaml:"tls"`
	SSLClientCRT   string `yaml:"ssl_client_crt"`
	SSLClientKey   string `yaml:"ssl_client_key"`
	SSLCACRT       string `yaml:"ssl_ca_crt"`
}
