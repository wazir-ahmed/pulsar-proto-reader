package reader

import (
	"fmt"
	"time"

	"github.com/accuknox/auto-policy-discovery/src/protobuf/v1/discovery"
	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type ConnectorOption struct {
	URL         string
	Topic       string
	ClusterName string
}

func PrintProtoFeeds(opt ConnectorOption) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               opt.URL,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	defer client.Close()

	channel := make(chan pulsar.ConsumerMessage, 1000)

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            opt.Topic,
		SubscriptionName: "pulsar-proto-reader-" + time.Now().Format(time.RFC3339Nano),
		MessageChannel:   channel,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for cm := range channel {
		msg := cm.Message.Payload()

		policy := discovery.GetPolicyResponse{}
		err := proto.Unmarshal(msg, &policy)
		if err != nil {
			log.Error(err)
			continue
		}

		fmt.Printf("New policy --> %#v\n", policy)
		consumer.Ack(cm.Message)
	}
}
