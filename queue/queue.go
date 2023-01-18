package queue

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/exp/slog"
)

type ConfigMQ struct {
	MQServer   string `json:"mqServer" binding:"required"`
	MQPort     string `json:"mqPort" binding:"required"`
	MQUser     string `json:"mqUser" binding:"required"`
	MQPassword string `json:"mqPassword" binding:"required"`
	MQSsl      bool   `json:"mqSsl"`
	MQProtocol string `json:"mqProtocol"`
	MQVhost    string `json:"mqVHot" binding:"required"`
}

type SetQueue struct {
	Ctx         context.Context `json:"ctx" binding:"required"`
	Queue       string          `json:"queue" binding:"required"`
	Connection  ConfigMQ        `json:"connection" binding:"required"`
	MessageType string          `json:"messageType" binding:"required,enum" enum:"send,receive"`
	Message     []byte          `json:"message" binding:"required"`
}

func (q *SetQueue) SendMessage(msg []byte) error {

	if err := q.send(); err != nil {
		log.Fatal(err, fmt.Sprintf("Error to publish the message on Message Queue Server ate channel (%v) ", q.Queue))
	}

	return nil
}

func (q *SetQueue) send() error {

	conn, err := q.connQueue()
	if err != nil {
		log.Fatal(err, fmt.Sprintf("Error to publish the message on Message Queue Server ate channel (%v) ", q.Queue))
	}
	err = q.declareQueue()
	if err != nil {
		log.Fatal(err, fmt.Sprintf("Error to publish the message on Message Queue Server ate channel (%v) ", q.Queue))
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err, "Error to connect on channel of Messsage Queue Server")
	}

	err = ch.PublishWithContext(q.Ctx,
		"",      // consumer
		q.Queue, // queue
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        q.Message,
		})

	if err != nil {
		log.Fatal(err, fmt.Sprintf("Error to publish the message on Message Queue Server ate channel (%v) ", q.Queue))
	}
	return err
}

func (q *SetQueue) declareQueue() error {

	conn, err := q.connQueue()
	if err != nil {
		log.Fatal(err, "Error to connect in Message Queue Server")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err, "Error to connect on channel of Messsage Queue Server")
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		q.Queue, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)

	return err

}

func (q *SetQueue) connQueue() (*amqp.Connection, error) {

	connString := fmt.Sprintf("%s://%s:%s@%s:%s/%s", q.Connection.MQProtocol, q.Connection.MQUser, q.Connection.MQPassword, q.Connection.MQServer, q.Connection.MQPort, q.Connection.MQVhost)
	conn, err := amqp.DialConfig(connString, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 1*time.Second)
		},
	})

	if err != nil {
		slog.Info("\nError to connect on Message Queue Server (%v) with username (%v) and password (%v)", q.Connection.MQServer, q.Connection.MQUser, q.Connection.MQPassword, slog.Duration("duration", time.Since(time.Now())))
		defer conn.Close()
		fmt.Println(err, fmt.Sprintf("Error to connect on Message Queue Server (%v) with username (%v) and password (%v)", q.Connection.MQServer, q.Connection.MQUser, q.Connection.MQPassword))
	}

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err, "Error to connect on Channel")
		defer ch.Close()
	}

	return conn, nil
}
