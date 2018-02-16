package main

import (
	"bitbucket.org/tapgerine/statistics_writer/consumer"
	"bitbucket.org/tapgerine/statistics_writer/consumer/database"

	"flag"

	"bitbucket.org/tapgerine/statistics_writer/job_queue"

	"os"

	"os/signal"

	"time"

	"fmt"

	"log/syslog"

	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/Sirupsen/logrus/hooks/syslog"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.InfoLevel)
	hook, err := logrus_syslog.NewSyslogHook("", "", syslog.LOG_INFO, "")
	if err != nil {
		log.Error("Unable to connect to local syslog daemon")
	} else {
		log.AddHook(hook)
	}
}

func main() {
	var (
		maxWorkers               = flag.Int("max_workers", 10, "The number of workers to start")
		maxQueueSize             = flag.Int("max_queue_size", 100000, "The size of job queue")
		kafkaBroker              = flag.String("kafka_broker", "localhost:9092", "Kafka brokers address")
		clickHouseClient         = flag.String("clickhouse_client", "localhost:8123", "ClickHouse client host")
		clickHouseReplica        = flag.String("clickhouse_replica", "localhost:8123", "ClickHouse client replica host")
		postgresUser             = flag.String("postgres_user", "pmp", "Postgres user name")
		postgresPwd              = flag.String("postgres_pwd", "", "Postgres user password")
		postgresHost             = flag.String("postgres_host", "localhost", "Postgres host")
		logFilePath              = flag.String("log_file", "/tmp/statistics_writer", "Log file for critical errors")
		isCriticalLoggingEnabled = flag.String("is_log_enabled", "false", "Is critical logging enabled")
	)
	flag.Parse()

	if *isCriticalLoggingEnabled == "true" {
		crashLogFileName := fmt.Sprintf(`%s_%d`, *logFilePath, time.Now().UTC().Unix())
		crashLogFile, _ := os.OpenFile(crashLogFileName, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0644)
		syscall.Dup2(int(crashLogFile.Fd()), 1)
		syscall.Dup2(int(crashLogFile.Fd()), 2)
	}

	var err error
	database.Postgres, err = gorm.Open(
		"postgres",
		fmt.Sprintf("host=%s dbname=pmp sslmode=disable user=%s password=%s", *postgresHost, *postgresUser, *postgresPwd),
	)
	if err != nil {
		log.WithError(err).Warn()
		return
	}
	defer database.Postgres.Close()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)

	kafkaJobQueue := make(chan job_queue.Job, *maxQueueSize)
	dispatcherKafkaJobs := job_queue.NewDispatcher(kafkaJobQueue, *maxWorkers)
	dispatcherKafkaJobs.Run()

	// Starting queue with multiple payloads
	// TODO: rewrite this code to have only one worker
	requestsJobQueue := make(chan job_queue.Job, *maxQueueSize)
	dispatcherRequestsJob := job_queue.NewDispatcher(requestsJobQueue, 1)
	dispatcherRequestsJob.RunWorkersWithMultiplePayloads(10000)

	rtbEventsJobQueue := make(chan job_queue.Job, *maxQueueSize)
	dispatcherRTBEventsJob := job_queue.NewDispatcher(rtbEventsJobQueue, 1)
	dispatcherRTBEventsJob.RunWorkersWithMultiplePayloads(10000)

	rtbBidRequestsJobQueue := make(chan job_queue.Job, *maxQueueSize)
	dispatcherRTBBidRequestsJob := job_queue.NewDispatcher(rtbBidRequestsJobQueue, 1)
	dispatcherRTBBidRequestsJob.RunWorkersWithMultiplePayloads(10000)

	eventsJobQueue := make(chan job_queue.Job, *maxQueueSize)
	dispatcherEventsJob := job_queue.NewDispatcher(eventsJobQueue, 1)
	dispatcherEventsJob.RunWorkersWithMultiplePayloads(1000)

	// Waiting for kill signal and trying to send all data that we received
	go func() {
		<-c
		log.Info("Stop signal received")
		dispatcherKafkaJobs.Stop()
		dispatcherRequestsJob.Stop()
		dispatcherRTBEventsJob.Stop()
		dispatcherRTBBidRequestsJob.Stop()
		dispatcherEventsJob.Stop()
		// TODO: add var wg sync.WaitGroup
		time.Sleep(10 * time.Second)
		os.Exit(1)
	}()

	// Every 60 seconds we send job that tell that we should flush workers payload pool
	go func() {
		for {
			time.Sleep(time.Second * 60)
			job := job_queue.Job{
				Name: "send_stats",
			}
			requestsJobQueue <- job
			eventsJobQueue <- job
			rtbEventsJobQueue <- job
			rtbBidRequestsJobQueue <- job
		}
	}()

	consumer.ReadStatsFromKafka(
		kafkaJobQueue,
		requestsJobQueue,
		rtbEventsJobQueue,
		rtbBidRequestsJobQueue,
		eventsJobQueue,
		*kafkaBroker,
		*clickHouseClient,
		*clickHouseReplica,
	)
}
