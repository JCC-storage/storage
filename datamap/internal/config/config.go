package config

import (
	"github.com/spf13/viper"
)

type Config struct {
	Database DatabaseConfig
	RabbitMQ RabbitMQConfig
	Server   ServerConfig
}

type DatabaseConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

type RabbitMQConfig struct {
	Host     string
	Port     string
	User     string
	Password string
}

type ServerConfig struct {
	Port string
}

func LoadConfig() *Config {
	viper.SetConfigFile("C:\\Users\\Administrator\\workspace\\workspace\\storage\\datamap\\.env")
	viper.ReadInConfig()

	return &Config{
		Database: DatabaseConfig{
			Host:     viper.GetString("DB_HOST"),
			Port:     viper.GetString("DB_PORT"),
			User:     viper.GetString("DB_USER"),
			Password: viper.GetString("DB_PASSWORD"),
			DBName:   viper.GetString("DB_NAME"),
		},
		RabbitMQ: RabbitMQConfig{
			Host:     viper.GetString("RABBITMQ_HOST"),
			Port:     viper.GetString("RABBITMQ_PORT"),
			User:     viper.GetString("RABBITMQ_USER"),
			Password: viper.GetString("RABBITMQ_PASSWORD"),
		},
		Server: ServerConfig{
			Port: viper.GetString("SERVER_PORT"),
		},
	}
}
