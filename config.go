package eventbusclient

import (
	"fmt"
	"github.com/kelseyhightower/envconfig"
	"html/template"
)

//Config Eventbus config
type Config struct {
	Host         string `envconfig:"EVENTBUS_HOST" required:"true"`
	Port         string `envconfig:"EVENTBUS_PORT" required:"true"`
	Username     string `envconfig:"EVENTBUS_USERNAME" required:"true"`
	Password     string `envconfig:"EVENTBUS_PASSWORD" required:"true"`
	PrefectCount int    `envconfig:"EVENTBUS_PREFECT_COUNT" required:"false" default:"50"`
}

// GetURL from config, build connection string
func (c Config) GetURL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/", c.Username, template.URLQueryEscaper(c.Password), c.Host, c.Port)
}

// GetAppConfigFromEnv Read system environment to get config
func GetAppConfigFromEnv() Config {
	var conf Config
	envconfig.MustProcess("", &conf)
	return conf
}
