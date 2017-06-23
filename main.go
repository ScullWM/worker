package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type DatabaseConfig struct {
	Connection struct {
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
		Name     string `yaml:"name"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	}
}

func main() {
	heartbeatIntervalString := GetEnvVar("HEARTBEAT_INTERVAL", "5")
	heartbeatInterval, err := strconv.Atoi(heartbeatIntervalString)
	if err != nil {
		panic(err)
	}

	JobPrint("worker", "Starting worker with heartbeat at "+heartbeatIntervalString+"sec")

	c := cron.New()
	c.AddFunc("@every 2h", ReferentManagedUsers)
	c.AddFunc("0 3 * * *", ClearMailjetEmails) // Every day at 3 am
	c.Start()

	ReferentManagedUsers()
	ClearMailjetEmails()

	for {
		time.Sleep(time.Second * time.Duration(heartbeatInterval))
		JobPrint("worker", "Heartbeat")
	}
}

func JobPrint(jobName string, message string) {
	fmt.Println(jobName + " | " + message)
}

func GetDatabaseConnection() *sql.DB {
	config, err := GetDatabaseConfiguration()
	if err != nil {
		panic(err)
	}

	dbHost := GetEnvVar("DATABASE_HOST", config.Connection.Host)
	dbPort := GetEnvVar("DATABASE_PORT", config.Connection.Port)
	dbName := GetEnvVar("DATABASE_NAME", config.Connection.Name)
	dbUser := GetEnvVar("DATABASE_USER", config.Connection.User)
	dbPass := GetEnvVar("DATABASE_PASSWORD", config.Connection.Password)
	dsn := dbUser + ":" + dbPass + "@tcp(" + dbHost + ":" + dbPort + ")/" + dbName

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}

	if db.Ping() != nil {
		panic(err)
	}

	return db
}

func GetDatabaseConfiguration() (*DatabaseConfig, error) {
	config := &DatabaseConfig{}

	bytes, err := ioutil.ReadFile("./config.yml")
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(bytes, config); err != nil {
		return nil, err
	}

	return config, nil
}

func GetEnvVar(key, fallback string) string {
	value := os.Getenv(key)

	if len(value) == 0 {
		return fallback
	}

	return value
}
