package queue

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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
	Debug       bool            `json:"debug"`
	Exchange    string          `json:"exchange"`
	Timeout     time.Duration   `json:"timeout"`
}

func (q *SetQueue) SendMessage(msg []byte) error {

	err := q.send()
	if err != nil {
		log.Print("Error to publish the message on Message Queue Server at queue (%v) and error is (%v) ", q.Queue, err.Error())
	}

	return err
}

func (q *SetQueue) send() error {

	conn, err := q.connQueue()
	if err != nil {
		log.Print("Error to publish the message on Message Queue Server at queue (%v)and error is (%v) ", q.Queue, err.Error())
	}
	err = q.declareQueue()
	if err != nil {
		log.Print("Error to publish the message on Message Queue Server at queue (%v)and error is (%v) ", q.Queue, err.Error())
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Print("Error to publish the message on Message Queue Server at queue (%v)and error is (%v) ", q.Queue, err.Error())
	}

	err = ch.PublishWithContext(q.Ctx,
		"discovery", // Exchange
		q.Queue,     // queue
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        q.Message,
		})

	if err != nil {
		log.Print("Error to publish the message on Message Queue Server at queue (%v)and error is (%v) ", q.Queue, err.Error())
	}
	return err
}

func (q *SetQueue) declareQueue() error {

	conn, err := q.connQueue()
	if err != nil {
		log.Print("Error to publish the message on Message Queue Server at queue (%v)and error is (%v) ", q.Queue, err.Error())
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Print("Error to publish the message on Message Queue Server at queue (%v)and error is (%v) ", q.Queue, err.Error())
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

	if q.Timeout == 0 {
		q.Timeout = time.Duration(15)
	}

	connString := fmt.Sprintf("%s://%s:%s@%s:%s/%s", q.Connection.MQProtocol, q.Connection.MQUser, q.Connection.MQPassword, q.Connection.MQServer, q.Connection.MQPort, q.Connection.MQVhost)
	if q.Debug {
		fmt.Printf("Error to connect in Message Queue Server", fmt.Sprintf("ConnectionSring", connString), fmt.Sprintf("duration %v", time.Since(time.Now())))
	}
	conn, err := amqp.DialConfig(connString, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, time.Duration(q.Timeout))
		},
	})

	if err != nil {
		if q.Debug {
			fmt.Printf("Error to connect on Message Queue Server",
				fmt.Sprintf("error", err.Error()),
				fmt.Sprintf("server", q.Connection.MQServer),
				fmt.Sprintf("port", q.Connection.MQPort),
				fmt.Sprintf("protocol", q.Connection.MQProtocol),
				fmt.Sprintf("sslEnabled", strconv.FormatBool(q.Connection.MQSsl)),
				fmt.Sprintf("vhost", q.Connection.MQVhost),
				fmt.Sprintf("user", q.Connection.MQUser),
				fmt.Sprintf("password", q.Connection.MQPassword),
				fmt.Sprintf("ConnectionSring", connString),
				fmt.Sprintf("duration %v", time.Since(time.Now())),
			)
		} else {
			fmt.Printf("\nError to connect on Message Queue Server (%v) with username (%v)", q.Connection.MQServer, q.Connection.MQUser, fmt.Sprintf("duration %v", time.Since(time.Now())))
		}
		defer conn.Close()
		fmt.Println(err, fmt.Sprintf("Error to connect on Message Queue Server (%v) with username (%v) and password (%v)", q.Connection.MQServer, q.Connection.MQUser, q.Connection.MQPassword))
	}

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err, "Error to connect on Channel")
		defer ch.Close()
	}

	return conn, err
}
