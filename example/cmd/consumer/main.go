package main

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/mickamy/txoutbox/example/internal/config"
	awstest "github.com/mickamy/txoutbox/test/aws"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	client, err := awstest.NewSQSClient(ctx, cfg.SQSEndpoint)
	if err != nil {
		log.Fatalf("init sqs client: %v", err)
	}

	log.Printf("consumer listening on queue %s", cfg.QueueURL)
	for {
		select {
		default:
		}

		resp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(cfg.QueueURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     5,
			VisibilityTimeout:   30,
		})
		if err != nil {
			log.Printf("receive error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if len(resp.Messages) == 0 {
			continue
		}
		for _, msg := range resp.Messages {
			log.Printf("received message: %s", aws.ToString(msg.Body))
			if msg.ReceiptHandle == nil {
				continue
			}
			if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(cfg.QueueURL),
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				log.Printf("delete error: %v", err)
			}
		}
	}
}
