package kinesis

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	kinesis_consumer "github.com/harlow/kinesis-consumer"
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

func InitKinesisConsumer(roleARN string, streamName string, notifStream chan []byte) (*context.CancelFunc, error) {

	client, err := initClient(roleARN)
	if err != nil {
		err := fmt.Errorf("init AWS-Kinesis client error: %v", err)
		logrus.Errorf("%v", err)
		return nil, err
	}
	logrus.Debugf("AWS-Kinesis client initialized")

	cancelCtx, cancelFunc := context.WithCancel(context.TODO())
	// Check if the stream exists
	{
		listStreamsOutput, err := client.ListStreams(
			cancelCtx,
			&kinesis.ListStreamsInput{},
		)
		if err != nil {
			err := fmt.Errorf("AWS-Kinesis ListStreams error: %v", err)
			logrus.Errorf("%v", err)
			cancelFunc()
			return nil, err
		}
		if contains(listStreamsOutput.StreamNames, streamName) {
			logrus.Debugf("the AWS-Kinesis Data Stream named ** %s ** exists", streamName)
		} else {
			err := fmt.Errorf("the AWS-Kinesis Data Stream named ** %s ** does not exist", streamName)
			logrus.Errorf("%v", err)
			cancelFunc()
			return nil, err
		}
	}

	// initialize consumer
	var c *kinesis_consumer.Consumer
	{
		logger := customizedLogger{
			logger: logrus.StandardLogger(),
		}
		c, err = kinesis_consumer.New(
			streamName,
			kinesis_consumer.WithClient(client),
			kinesis_consumer.WithLogger(&logger),
			kinesis_consumer.WithShardIteratorType(string(types.ShardIteratorTypeAtTimestamp)),
			kinesis_consumer.WithTimestamp(time.Now()),
		)
	}
	if err != nil {
		err := fmt.Errorf("create AWS-Kinesis consumer error: %v", err)
		logrus.Errorf("%v", err)
		cancelFunc()
		return nil, err
	}
	logrus.Debugf(
		"AWS-Kinesis consumer initialized on stream ** %s **",
		streamName,
	)

	go func(ctx context.Context) {
		err = c.Scan(
			ctx,
			func(r *kinesis_consumer.Record) error {
				numberOfBytes := len(r.Data)
				notifStream <- r.Data
				logrus.Debugf("record received, %d bytes", numberOfBytes)
				logrus.Info("COUCOU...")
				return nil
			},
		)
		logrus.Info("BYE...")
		if err != nil {
			logrus.Errorf("scan error: %v", err)
		}
	}(cancelCtx)

	return &cancelFunc, nil
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
