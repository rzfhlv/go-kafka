package main

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

type Order struct {
	ID        string `json:"id"`
	ProductID string `json:"product_id"`
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
}

const (
	KafkaServer  = "localhost:29092"
	KafkaTopic   = "orders-v1-topic"
	KafkaGroupId = "product-service"
)

func main() {
	fmt.Println("Hello Kafka!")
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": KafkaServer,
	})
	if err != nil {
		panic(err.Error())
	}
	defer p.Close()

	fmt.Println("producer ready!")

	topic := KafkaTopic
	order := Order{
		ID:        uuid.New().String(),
		ProductID: uuid.New().String(),
		UserID:    uuid.New().String(),
		Amount:    456000,
	}

	value, err := json.Marshal(order)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("value ready!")

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
	}, nil)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("publish value")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": KafkaServer,
		"group.id":          KafkaGroupId,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		panic(err.Error())
	}
	defer c.Close()

	c.SubscribeTopics([]string{topic}, nil)

	fmt.Println("start subscribe")

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			var cOrder Order
			err := json.Unmarshal(msg.Value, &cOrder)
			if err != nil {
				fmt.Printf("Error decoding message: %v\n", err)
				continue
			}
			fmt.Printf("Recieved Order: %+v\n", cOrder)
		} else {
			fmt.Printf("Error: %v\n", err)
		}
	}
}
