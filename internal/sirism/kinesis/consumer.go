package kinesis

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	consumer "github.com/harlow/kinesis-consumer"
	"github.com/sirupsen/logrus"
)

// Returns `true` if the string `x` is contained in `list`,
// otherwise it returns `false`
func contains(list []string, x string) bool {
	for _, item := range list {
		if item == x {
			return true
		}
	}
	return false
}

// This `struct` implements the interger `consumer.Logger`,
// required to implement the fun `consumer.WithLogger`
type customizedLogger struct {
	logger *logrus.Logger
}

func (l *customizedLogger) Log(args ...interface{}) {
	l.logger.Println(args...)
}

func InitKinesisConsumer(
	roleARN string,
	streamName string,
	notifStream chan []byte,
	notificationsReloadPeriod time.Duration,
) {

	client, err := initClient(roleARN)
	if err != nil {
		logrus.Errorf("init AWS-Kinesis client error: %v", err)
		return
	}
	logrus.Debugf("AWS-Kinesis client initialized")

	// Check if the stream exists
	{
		listStreamsOutput, err := client.ListStreams(
			context.Background(),
			&kinesis.ListStreamsInput{},
		)
		if err != nil {
			logrus.Errorf("AWS-Kinesis ListStreams error: %v", err)
			return
		}
		if contains(listStreamsOutput.StreamNames, streamName) {
			logrus.Debugf("the AWS-Kinesis Data Stream named ** %s ** exists", streamName)
		} else {
			logrus.Errorf("the AWS-Kinesis Data Stream named ** %s ** does not exist", streamName)
			return
		}
	}

	// initialize consumer
	var c *consumer.Consumer
	{
		now := time.Now().UTC()
		timestamp := now.Add(-notificationsReloadPeriod)
		logger := customizedLogger{
			logger: logrus.StandardLogger(),
		}
		logrus.Infof(
			"reload latest Kinesis records since %s (%v in the past)",
			notificationsReloadPeriod,
			timestamp.Format(time.RFC3339),
		)
		c, err = consumer.New(
			streamName,
			consumer.WithClient(client),
			consumer.WithLogger(&logger),
			consumer.WithShardIteratorType(string(types.ShardFilterTypeAtTimestamp)),
			consumer.WithTimestamp(timestamp),
			consumer.WithScanInterval(1*time.Millisecond),
			consumer.WithMaxRecords(10_000),
		)
	}
	if err != nil {
		logrus.Errorf("create AWS-Kinesis consumer error: %v", err)
		return
	}
	logrus.Debugf(
		"AWS-Kinesis consumer initialized on stream ** %s **",
		streamName,
	)

	go func() {
		err = c.Scan(
			context.Background(),
			func(r *consumer.Record) error {
				numberOfBytes := len(r.Data)
				notifStream <- r.Data
				logrus.Debugf("record received, %d bytes", numberOfBytes)
				return nil
			},
		)
		if err != nil {
			logrus.Errorf("scan error: %v", err)
		}
	}()
}

func initClient(roleARN string) (*kinesis.Client, error) {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		// config.WithRegion(awsRegion),
	)
	if err != nil {
		return nil, err
	}

	if roleARN != "" {
		logrus.Infof("try to assume role '%s'", roleARN)
		stsclient := sts.NewFromConfig(cfg)
		assumed_cfg, assumed_err := config.LoadDefaultConfig(
			context.TODO(),
			config.WithCredentialsProvider(
				aws.NewCredentialsCache(
					stscreds.NewAssumeRoleProvider(
						stsclient,
						roleARN,
					)),
			),
		)
		if assumed_err != nil {
			return nil, assumed_err
		}
		cfg = assumed_cfg
	}

	var client *kinesis.Client = kinesis.NewFromConfig(cfg)
	return client, nil
}
