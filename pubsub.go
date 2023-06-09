package main

import (
	"context"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/mitchellh/mapstructure"
	"go.k6.io/k6/js/modules"
	pubsub_option "google.golang.org/api/option"
)

func init() {
	modules.Register("k6/x/pubsub", newK6PubSub())
}

type k6PubSub struct{}

type k6PubSubConfig struct {
	ProjectID        string `mapstructure:"project_id"`
	CredentialFile   string `mapstructure:"credential_file"`
	CredentialJSON   string `mapstructure:"credential_json"`
	Endpoint         string `mapstructure:"endpoint"`
	DisableTelemetry bool   `mapstructure:"disable_telemetry"`
	NoAuthentication bool   `mapstructure:"no_authentication"`
}

func newK6PubSub() *k6PubSub {
	return &k6PubSub{}
}

// Publish publishes a message to Google PubSub with the provided
// configuration. An example script for publishing
func (ps *k6PubSub) Publish(ctx context.Context, topic, msg string,
	config map[string]interface{}) error {

	clientConfig := k6PubSubConfig{}
	err := mapstructure.Decode(config, &clientConfig)
	if err != nil {
		log.Fatalf("failed to decoded config: %v", err)
	}

	client, err := ps.client(&clientConfig)
	if err != nil {
		log.Fatalf("failed to create pubsub client: %v", err)
	}

	defer client.Close()
	client.Topic(topic).Publish(ctx, &pubsub.Message{
		Data: []byte(msg),
	})

	return nil
}

func (ps *k6PubSub) client(config *k6PubSubConfig) (*pubsub.Client, error) {
	options := []pubsub_option.ClientOption{}

	if len(config.CredentialFile) > 0 {
		options = append(options, pubsub_option.WithCredentialsFile(config.CredentialFile))
	}

	if len(config.CredentialJSON) > 0 {
		options = append(options, pubsub_option.WithCredentialsJSON([]byte(config.CredentialJSON)))
	}

	client, err := pubsub.NewClient(context.Background(), config.ProjectID, options...)
	if err != nil {
		return nil, err
	}

	return client, nil
}
