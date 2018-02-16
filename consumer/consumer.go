package consumer

import (
	"os"

	"bitbucket.org/tapgerine/statistics_writer/job_queue"

	"time"

	"sync"

	"strings"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/patrickmn/go-cache"
	"github.com/roistat/go-clickhouse"
)

func ReadStatsFromKafka(
	kafkaJobQueue chan job_queue.Job,
	requestsJobQueue chan job_queue.Job,
	rtbEventsJobQueue chan job_queue.Job,
	rtbBidRequestsJobQueue chan job_queue.Job,
	eventsJobQueue chan job_queue.Job,
	kafkaBroker,
	clickHouseClient, clickHouseReplica string,
) {
	log.Info("Work started")
	brokers := strings.Split(kafkaBroker, ",")
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.MaxWaitTime = 1000 * time.Millisecond
	config.Consumer.MaxProcessingTime = 1000 * time.Millisecond

	c, _ := sarama.NewConsumer(brokers, config)
	c2, _ := sarama.NewConsumer(brokers, config)
	c3, _ := sarama.NewConsumer(brokers, config)

	requestsConsumer, err := c.ConsumePartition("requests", 0, sarama.OffsetNewest)
	if err != nil {
		log.WithField("error", "Can't connect to kafka").Fatal(err)
		panic(err)
	}
	requestsTargetingConsumer, err := c.ConsumePartition("requests_targeting", 0, sarama.OffsetNewest)
	if err != nil {
		log.WithField("error", "Can't connect to kafka").Fatal(err)
		panic(err)
	}
	eventsConsumer, err := c2.ConsumePartition("events", 0, sarama.OffsetNewest)
	if err != nil {
		log.WithField("error", "Can't connect to kafka").Fatal(err)
		panic(err)
	}
	rtbEventsConsumer, err := c3.ConsumePartition("rtb_events", 0, sarama.OffsetNewest)
	if err != nil {
		log.WithField("error", "Can't connect to kafka").Fatal(err)
		panic(err)
	}
	rtbBidRequestsConsumer, err := c3.ConsumePartition("rtb_bid_requests", 0, sarama.OffsetNewest)
	if err != nil {
		log.WithField("error", "Can't connect to kafka").Fatal(err)
		panic(err)
	}

	http := clickhouse.NewHttpTransport()
	http.Timeout = time.Second * 60

	conn1 := clickhouse.NewConn(clickHouseClient, http)
	//conn2 := clickhouse.NewConn(clickHouseReplica, http)
	clickHouseConnection := clickhouse.NewCluster(conn1)
	clickHouseConnection.OnCheckError(func(c *clickhouse.Conn) {
		log.Fatalf("Clickhouse connection failed %s", c.Host)
	})

	// Ping connections every second
	go func() {
		for {
			clickHouseConnection.Check()
			time.Sleep(time.Second)
		}
	}()

	clickHouseWriterLock := sync.Mutex{}

	adTagCache := cache.New(5*time.Minute, 10*time.Minute)

	for {
		select {
		case msg := <-requestsConsumer.Messages():
			job := job_queue.Job{
				Name: "request",
				Payload: payloadKafkaJob{
					Connection:           clickHouseConnection,
					Message:              *msg,
					AdTagCache:           adTagCache,
					JobQueue:             requestsJobQueue,
					ClickHouseWriterLock: clickHouseWriterLock,
				},
				Processor: processKafkaRequestsMessageJob,
			}
			kafkaJobQueue <- job
		case msg := <-rtbEventsConsumer.Messages():
			job := job_queue.Job{
				Name: "rtb_events",
				Payload: payloadKafkaJob{
					Connection:           clickHouseConnection,
					Message:              *msg,
					JobQueue:             rtbEventsJobQueue,
					ClickHouseWriterLock: clickHouseWriterLock,
				},
				Processor: processKafkaRTBEventMessageJob,
			}
			kafkaJobQueue <- job
		case msg := <-rtbBidRequestsConsumer.Messages():
			job := job_queue.Job{
				Name: "rtb_bid_requests",
				Payload: payloadKafkaJob{
					Connection:           clickHouseConnection,
					Message:              *msg,
					JobQueue:             rtbBidRequestsJobQueue,
					ClickHouseWriterLock: clickHouseWriterLock,
				},
				Processor: processKafkaRTBBidRequestsMessageJob,
			}
			kafkaJobQueue <- job
		case msg := <-requestsTargetingConsumer.Messages():
			job := job_queue.Job{
				Name: "requests_targeting",
				Payload: payloadKafkaJob{
					Connection:           clickHouseConnection,
					Message:              *msg,
					AdTagCache:           adTagCache,
					JobQueue:             requestsJobQueue,
					ClickHouseWriterLock: clickHouseWriterLock,
				},
				Processor: processKafkaRequestsMessageJob,
			}
			kafkaJobQueue <- job
		case msg := <-eventsConsumer.Messages():
			job := job_queue.Job{
				Name: "events",
				Payload: payloadKafkaJob{
					Connection:           clickHouseConnection,
					Message:              *msg,
					AdTagCache:           adTagCache,
					JobQueue:             eventsJobQueue,
					ClickHouseWriterLock: clickHouseWriterLock,
				},
				Processor: processKafkaEventsMessageJob,
			}
			kafkaJobQueue <- job
		case err := <-requestsConsumer.Errors():
			log.WithError(err).Warn()
			exit()
		case err := <-requestsTargetingConsumer.Errors():
			log.WithError(err).Warn()
			exit()
		case err := <-eventsConsumer.Errors():
			log.WithError(err).Warn()
			exit()
		}
	}
}

func exit() {
	log.Warn("Stopping program because of kafka error")
	proc, _ := os.FindProcess(os.Getpid())
	proc.Signal(os.Interrupt)
}
