package kinesis

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	consumer "github.com/harlow/kinesis-consumer"
	"github.com/sirupsen/logrus"
)

func InitKinesisConsumer(streamName string, notifStream chan []byte) {

	client, err := initClient()
	if err != nil {
		logrus.Errorf("init AWS-Kinesis client error: %v", err)
		return
	}
	logrus.Debugf(
		"AWS-Kinesis client initialized: (stream: %s",
		streamName,
	)

	// initialize consumer
	c, err := consumer.New(
		streamName,
		consumer.WithClient(client),
	)
	if err != nil {
		logrus.Errorf("create AWS-Kinesis consumer error: %v", err)
		return
	}
	logrus.Debugf(
		"AWS-Kinesis consumer initialized: (stream: %s)",
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

func initClient() (*kinesis.Client, error) {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		// config.WithRegion(awsRegion),
	)
	if err != nil {
		return nil, err
	}
	var client *kinesis.Client = kinesis.NewFromConfig(cfg)
	return client, nil
}
