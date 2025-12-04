package model
import("time")

type TestDocument struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Value int       `json:"value"`
	Ts    time.Time `json:"timestamp"`
}
type ESConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}
