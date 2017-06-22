package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron"
)

func main() {
	var heartbeatIntervalString = GetEnvVar("HEARTBEAT_INTERVAL", "5")
	heartbeatInterval, err := strconv.Atoi(heartbeatIntervalString)
	if err != nil {
		panic(err.Error())
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
	var dbHost = GetEnvVar("DATABASE_HOST", "127.0.0.1")
	var dbPort = GetEnvVar("DATABASE_PORT", "3306")
	var dbName = GetEnvVar("DATABASE_NAME", "enmarche")
	var dbUser = GetEnvVar("DATABASE_USER", "root")
	var dbPass = GetEnvVar("DATABASE_PASSWORD", "root")
	var dsn = dbUser + ":" + dbPass + "@tcp(" + dbHost + ":" + dbPort + ")/" + dbName

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err.Error())
	}

	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}

	return db
}

func GetEnvVar(key, fallback string) string {
	value := os.Getenv(key)

	if len(value) == 0 {
		return fallback
	}

	return value
}
