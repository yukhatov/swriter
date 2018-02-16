package consumer

import (
	"errors"
	"time"

	"bitbucket.org/tapgerine/statistics_writer/consumer/database"

	"bitbucket.org/tapgerine/statistics_writer/job_queue"

	"sync"

	"math"

	"encoding/json"

	"bitbucket.org/tapgerine/pmp/control/models"
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/patrickmn/go-cache"
	"github.com/roistat/go-clickhouse"
)

type payloadKafkaJob struct {
	Message              sarama.ConsumerMessage
	AdTagCache           *cache.Cache
	Connection           *clickhouse.Cluster
	JobQueue             chan job_queue.Job
	ClickHouseWriterLock sync.Mutex
}

type payloadClickHouseJob struct {
	Connection           *clickhouse.Cluster
	Row                  clickhouse.Row
	ClickHouseWriterLock sync.Mutex
}

type KafkaRequestMessageFormat struct {
	MessageType string `json:"message_type"`
	AdTagPubID  string `json:"adpid"`
	RequestID   string `json:"rid"`
	Timestamp   int64  `json:"timestamp"`
	RequestType string `json:"rtype"`
	GeoCountry  string `json:"geo_country"`
	DeviceType  string `json:"device_type"`
	PublisherID uint64 `json:"publisher_id"`
	TargetingID string `json:"targeting_id"`
	Domain      string `json:"domain"`
	AppName     string `json:"app_name"`
	BundleID    string `json:"bundle_id"`
}

type EventParams struct {
	EventName    string  `json:"e"`
	RequestID    string  `json:"r"`
	AdTagPubID   string  `json:"atpid"`
	Price        float64 `json:"p"`
	RequestType  string  `json:"r_type"`
	GeoCountry   string  `json:"geo"`
	DeviceType   string  `json:"d"`
	TargetingID  string  `json:"tid"`
	Domain       string  `json:"domain"`
	AppName      string  `json:"app"`
	BundleID     string  `json:"bid"`
	Timestamp    int64   `json:"t"`
	AdvertiserID uint64  `json:"a_id"`
	PublisherID  uint64  `json:"p_id"`
	OriginPrice  float64 `json:"or_price"`
}

type KafkaRTBEventsMessageFormat struct {
	ID             string  `json:"id"`
	PublisherID    uint64  `json:"pid"`
	TargetingID    string  `json:"tid"`
	EventName      string  `json:"e"`
	PublisherPrice float64 `json:"price"`
	Timestamp      int64   `json:"timestamp"`
	GeoCountry     string  `json:"geo_country"`
	DeviceType     string  `json:"device_type"`
	Domain         string  `json:"domain"`
	AppName        string  `json:"app_name"`
	BundleID       string  `json:"bundle_id"`
}

type KafkaRTBBidRequestsMessageFormat struct {
	ID                 string  `json:"id"`
	PublisherID        uint64  `json:"pid"`
	TargetingID        string  `json:"tid"`
	PublisherPrice     float64 `json:"price"`
	Timestamp          int64   `json:"timestamp"`
	AdvertiserID       uint64  `json:"advertiser_id"`
	BidResponse        int8    `json:"bid_response"`
	BidResponseTime    int64   `json:"bid_response_time"`
	BidResponseTimeout int8    `json:"bid_response_timeout"`
	BidResponseEmpty   int8    `json:"bid_response_empty"`
	BidResponseError   string  `json:"bid_response_error"`
	BidWin             int8    `json:"bid_win"`
	BidFloorPrice      float64 `json:"bid_floor_price"`
	BidPrice           float64 `json:"bid_price"`
	SecondPrice        float64 `json:"second_price"`
	GeoCountry         string  `json:"geo_country"`
	DeviceType         string  `json:"device_type"`
	Domain             string  `json:"domain"`
	AppName            string  `json:"app_name"`
	BundleID           string  `json:"bundle_id"`
}

func processKafkaRequestsMessageJob(p interface{}) {
	data := p.(payloadKafkaJob)
	message := data.Message
	adTagCache := data.AdTagCache
	requestsJobQueue := data.JobQueue

	var msg KafkaRequestMessageFormat
	if err := json.Unmarshal(message.Value, &msg); err != nil {
		log.WithFields(log.Fields{"message": string(message.Value)}).WithError(err).Warn()
		return
	}

	timestamp := time.Unix(msg.Timestamp, 0).UTC()
	var adTagPub models.AdTagPublisher
	var err error
	if msg.AdTagPubID != "" {
		adTagPub, err = getAdTagPubFromDB(msg.AdTagPubID, adTagCache)
		if err != nil {
			log.WithFields(log.Fields{"message": string(message.Value)}).WithError(err).Warn()
			return
		}
	} else {
		adTagPub = models.AdTagPublisher{
			PublisherID: msg.PublisherID,
		}
	}

	job := job_queue.Job{
		Name: "click_house",
		Payload: payloadClickHouseJob{
			Connection: data.Connection,
			Row: clickhouse.Row{
				msg.RequestID, msg.AdTagPubID, adTagPub.AdTagID, adTagPub.PublisherID,
				adTagPub.AdTag.AdvertiserID,
				timestamp.Format("2006-01-02"),
				msg.Timestamp, msg.RequestType, msg.GeoCountry, msg.DeviceType, msg.TargetingID, msg.Domain,
				msg.AppName, msg.BundleID,
			},
			ClickHouseWriterLock: data.ClickHouseWriterLock,
		},
		Processor: processClickHouseRequestsInsertJob,
	}

	requestsJobQueue <- job
}

func processKafkaEventsMessageJob(p interface{}) {
	data := p.(payloadKafkaJob)
	message := data.Message
	adTagCache := data.AdTagCache
	eventsJobQueue := data.JobQueue

	var msg EventParams
	if err := json.Unmarshal(message.Value, &msg); err != nil {
		log.WithFields(log.Fields{"message": string(message.Value)}).WithError(err).Warn()
		return
	}

	timestamp := time.Unix(msg.Timestamp, 0).UTC()

	var adTagPub models.AdTagPublisher
	var advertiserID, publisherID uint64
	var err error

	if msg.AdTagPubID != "" {
		adTagPub, err = getAdTagPubFromDB(msg.AdTagPubID, adTagCache)
		if err != nil {
			log.WithFields(log.Fields{"message": string(message.Value)}).WithError(err).Warn()
			return
		}
		advertiserID = adTagPub.AdTag.AdvertiserID
		publisherID = adTagPub.PublisherID
	} else {
		advertiserID = msg.AdvertiserID
		publisherID = msg.PublisherID
	}

	if err != nil {
		log.WithFields(log.Fields{"message": string(message.Value)}).WithError(err).Warn()
		return
	}

	var originalPrice float64
	if msg.Price > 0 {
		if msg.OriginPrice > 0 {
			originalPrice = msg.OriginPrice / 1000.0 * 1000000.0
		} else {
			originalPrice = adTagPub.AdTag.Price / 1000.0 * 1000000.0
		}
	} else {
		originalPrice = 0.0
	}

	job := job_queue.Job{
		Name: "click_house_event",
		Payload: payloadClickHouseJob{
			Connection: data.Connection,
			Row: clickhouse.Row{
				msg.RequestID, msg.AdTagPubID, adTagPub.AdTagID,
				publisherID, advertiserID,
				timestamp.Format("2006-01-02"),
				msg.Timestamp,
				msg.EventName,
				math.Ceil(msg.Price / 1000.0 * 1000000.0),
				math.Ceil(originalPrice),
				msg.RequestType,
				msg.GeoCountry,
				msg.DeviceType,
				msg.TargetingID,
				msg.Domain,
				msg.AppName,
				msg.BundleID,
			},
			ClickHouseWriterLock: data.ClickHouseWriterLock,
		},
		Processor: processClickHouseEventsInsertJob,
	}

	eventsJobQueue <- job
}

func processKafkaRTBBidRequestsMessageJob(p interface{}) {
	data := p.(payloadKafkaJob)
	message := data.Message
	rtbBidRequestsJobQueue := data.JobQueue

	var msg KafkaRTBBidRequestsMessageFormat
	if err := json.Unmarshal(message.Value, &msg); err != nil {
		log.WithFields(log.Fields{"message": string(message.Value)}).WithError(err).Warn("5")
		return
	}

	timestamp := time.Unix(msg.Timestamp, 0).UTC()

	job := job_queue.Job{
		Name: "click_house_rtb_bid_requests",
		Payload: payloadClickHouseJob{
			Connection: data.Connection,
			Row: clickhouse.Row{
				msg.ID,
				msg.PublisherID,
				msg.TargetingID,
				timestamp.Format("2006-01-02"),
				msg.Timestamp,
				math.Ceil(msg.PublisherPrice / 1000.0 * 1000000.0),
				msg.AdvertiserID,
				msg.BidResponse,
				msg.BidResponseTime,
				msg.BidResponseTimeout,
				msg.BidResponseEmpty,
				msg.BidResponseError,
				msg.BidWin,
				math.Ceil(msg.BidFloorPrice / 1000.0 * 1000000.0),
				math.Ceil(msg.BidPrice / 1000.0 * 1000000.0),
				math.Ceil(msg.SecondPrice / 1000.0 * 1000000.0),
				msg.GeoCountry,
				msg.DeviceType,
				msg.Domain,
				msg.AppName,
				msg.BundleID,
			},
			ClickHouseWriterLock: data.ClickHouseWriterLock,
		},
		Processor: processClickHouseRTBBidRequestsInsertJob,
	}

	rtbBidRequestsJobQueue <- job
}

func processKafkaRTBEventMessageJob(p interface{}) {
	data := p.(payloadKafkaJob)
	message := data.Message
	//adTagCache := data.AdTagCache
	rtbJobQueue := data.JobQueue

	var msg KafkaRTBEventsMessageFormat
	if err := json.Unmarshal(message.Value, &msg); err != nil {
		log.WithFields(log.Fields{"message": string(message.Value)}).WithError(err).Warn()
		return
	}
	timestamp := time.Unix(msg.Timestamp, 0).UTC()

	job := job_queue.Job{
		Name: "click_house_rtb_event",
		Payload: payloadClickHouseJob{
			Connection: data.Connection,
			Row: clickhouse.Row{
				msg.ID,
				msg.PublisherID,
				msg.TargetingID,
				msg.EventName,
				timestamp.Format("2006-01-02"),
				msg.Timestamp,
				math.Ceil(msg.PublisherPrice / 1000.0 * 1000000.0),
				msg.GeoCountry,
				msg.DeviceType,
				msg.Domain,
				msg.AppName,
				msg.BundleID,
			},
			ClickHouseWriterLock: data.ClickHouseWriterLock,
		},
		Processor: processClickHouseRTBEventsInsertJob,
	}

	rtbJobQueue <- job
}

func processClickHouseRequestsInsertJob(p interface{}) {
	payload := p.([]interface{})

	var data []payloadClickHouseJob
	data = make([]payloadClickHouseJob, len(payload))

	for i, item := range payload {
		data[i] = item.(payloadClickHouseJob)
	}

	clickHouseConnection := data[0].Connection
	clickHouseWriterLock := data[0].ClickHouseWriterLock

	var clickHouseRows []clickhouse.Row
	clickHouseRows = make([]clickhouse.Row, len(data))

	for i, item := range data {
		clickHouseRows[i] = item.Row
	}

	query, err := clickhouse.BuildMultiInsert("statistics.daily_statistics",
		clickhouse.Columns{
			"request_id", "ad_tag_publisher_id", "ad_tag_id", "publisher_id", "advertiser_id", "date", "date_time",
			"request_type", "geo_country", "device_type", "targeting_id", "domain", "app_name", "bundle_id",
		},
		clickHouseRows,
	)

	if err == nil {
		clickHouseWriterLock.Lock()
		err = query.Exec(clickHouseConnection.ActiveConn())
		clickHouseWriterLock.Unlock()
		if err != nil {
			log.Info("Insert failed, trying one more time")
			clickHouseWriterLock.Lock()
			err = query.Exec(clickHouseConnection.ActiveConn())
			clickHouseWriterLock.Unlock()
			if err != nil {
				log.WithError(err).Warn()
			}
		}
	} else {
		log.WithFields(log.Fields{"query2": query, "args": clickHouseRows}).WithError(err).Warn()
	}
}

func processClickHouseEventsInsertJob(p interface{}) {
	payload := p.([]interface{})

	var data []payloadClickHouseJob
	data = make([]payloadClickHouseJob, len(payload))

	for i, item := range payload {
		data[i] = item.(payloadClickHouseJob)
	}

	clickHouseConnection := data[0].Connection
	clickHouseWriterLock := data[0].ClickHouseWriterLock

	var clickHouseRows []clickhouse.Row
	clickHouseRows = make([]clickhouse.Row, len(data))

	for i, item := range data {
		clickHouseRows[i] = item.Row
	}

	query, err := clickhouse.BuildMultiInsert("statistics.daily_statistics_events",
		clickhouse.Columns{
			"request_id",
			"ad_tag_publisher_id",
			"ad_tag_id",
			"publisher_id",
			"advertiser_id",
			"date",
			"date_time",
			"event_name",
			"amount",
			"origin_amount",
			"request_type",
			"geo_country",
			"device_type",
			"targeting_id",
			"domain",
			"app_name",
			"bundle_id",
		},
		clickHouseRows,
	)

	if err == nil {
		clickHouseWriterLock.Lock()
		err = query.Exec(clickHouseConnection.ActiveConn())
		clickHouseWriterLock.Unlock()
		if err != nil {
			log.WithFields(log.Fields{"query": query}).WithError(err).Warn()
		}
	} else {
		log.WithFields(log.Fields{"query": query}).WithError(err).Warn()
	}
}

func processClickHouseRTBEventsInsertJob(p interface{}) {
	payload := p.([]interface{})

	var data []payloadClickHouseJob
	data = make([]payloadClickHouseJob, len(payload))

	for i, item := range payload {
		data[i] = item.(payloadClickHouseJob)
	}

	clickHouseConnection := data[0].Connection
	clickHouseWriterLock := data[0].ClickHouseWriterLock

	var clickHouseRows []clickhouse.Row
	clickHouseRows = make([]clickhouse.Row, len(data))

	for i, item := range data {
		clickHouseRows[i] = item.Row
	}

	query, err := clickhouse.BuildMultiInsert("statistics.daily_rtb_events",
		clickhouse.Columns{
			"id",
			"publisher_id",
			"targeting_id",
			"event_name",
			"date",
			"date_time",
			"publisher_price",
			"geo_country",
			"device_type",
			"domain",
			"app_name",
			"bundle_id",
		},
		clickHouseRows,
	)

	if err == nil {
		clickHouseWriterLock.Lock()
		err = query.Exec(clickHouseConnection.ActiveConn())
		clickHouseWriterLock.Unlock()
		if err != nil {
			log.WithFields(log.Fields{"query": query}).WithError(err).Warn()
		}
	} else {
		log.WithFields(log.Fields{"query": query}).WithError(err).Warn()
	}
}

func processClickHouseRTBBidRequestsInsertJob(p interface{}) {
	payload := p.([]interface{})

	var data []payloadClickHouseJob
	data = make([]payloadClickHouseJob, len(payload))

	for i, item := range payload {
		data[i] = item.(payloadClickHouseJob)
	}

	clickHouseConnection := data[0].Connection
	clickHouseWriterLock := data[0].ClickHouseWriterLock

	var clickHouseRows []clickhouse.Row
	clickHouseRows = make([]clickhouse.Row, len(data))

	for i, item := range data {
		clickHouseRows[i] = item.Row
	}

	query, err := clickhouse.BuildMultiInsert("statistics.daily_rtb_bid_requests",
		clickhouse.Columns{
			"id",
			"publisher_id",
			"targeting_id",
			"date",
			"date_time",
			"publisher_price",
			"advertiser_id",
			"bid_response",
			"bid_response_time",
			"bid_response_timeout",
			"bid_response_empty",
			"bid_response_error",
			"bid_win",
			"bid_floor_price",
			"bid_price",
			"second_price",
			"geo_country",
			"device_type",
			"domain",
			"app_name",
			"bundle_id",
		},
		clickHouseRows,
	)

	if err == nil {
		clickHouseWriterLock.Lock()
		err = query.Exec(clickHouseConnection.ActiveConn())
		clickHouseWriterLock.Unlock()
		if err != nil {
			log.WithFields(log.Fields{"query": query}).WithError(err).Warn()
		}
	} else {
		log.WithFields(log.Fields{"query": query}).WithError(err).Warn()
	}
}

func getAdTagPubFromDB(adTagPubID string, adTagCache *cache.Cache) (models.AdTagPublisher, error) {
	if x, found := adTagCache.Get(adTagPubID); found {
		return x.(models.AdTagPublisher), nil
	} else {
		adTagPub := &models.AdTagPublisher{ID: adTagPubID}
		database.Postgres.Preload("AdTag").First(adTagPub)

		if adTagPub.ID == "" {
			return models.AdTagPublisher{}, errors.New("no data in db")
		}
		adTagCache.Set(adTagPubID, *adTagPub, 0)

		return *adTagPub, nil
	}
}
