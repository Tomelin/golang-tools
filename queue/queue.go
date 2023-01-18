package queue

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
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
	Debug       bool            `json:"debug"`
	Exchange    string          `json:"exchange"`
}

var (
	textHandler = slog.NewJSONHandler(os.Stdout)
	logger      = slog.New(textHandler)
)

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
		q.Exchange, // Exchange
		q.Queue,    // queue
		false,      // mandatory
		false,      // immediate
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

	connString := fmt.Sprintf("%s://%s:%s@%s:%s/%s", q.Connection.MQProtocol, q.Connection.MQUser, q.Connection.MQPassword, q.Connection.MQServer, q.Connection.MQPort, q.Connection.MQVhost)
	if q.Debug {
		logger.Debug("Error to connect in Message Queue Server", slog.String("ConnectionSring", connString), slog.Duration("duration", time.Since(time.Now())))
	}
	conn, err := amqp.DialConfig(connString, amqp.Config{
		Dial: func(network, addr string) (net.Conn, error) {
			return net.DialTimeout(network, addr, 1*time.Second)
		},
	})

	if err != nil {
		if q.Debug {
			logger.Debug("Error to connect on Message Queue Server",
				slog.String("error", err.Error()),
				slog.String("server", q.Connection.MQServer),
				slog.String("port", q.Connection.MQPort),
				slog.String("protocol", q.Connection.MQProtocol),
				slog.String("sslEnabled", strconv.FormatBool(q.Connection.MQSsl)),
				slog.String("vhost", q.Connection.MQVhost),
				slog.String("user", q.Connection.MQUser),
				slog.String("password", q.Connection.MQPassword),
				slog.String("ConnectionSring", connString),
				slog.Duration("duration", time.Since(time.Now())),
			)
		} else {
			slog.Info("\nError to connect on Message Queue Server (%v) with username (%v)", q.Connection.MQServer, q.Connection.MQUser, slog.Duration("duration", time.Since(time.Now())))
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
