package queue

import "context"

type ConfigMQ struct {
	MQServer   string
	MQPort     string
	MQUser     string
	MQPassword string
	MQSsl      bool
	MQProtocol string
	MQVhost    string
}

type SetQueue struct {
	Ctx        context.Context `json:"ctx"`
	Queue      string          `json:"queue"`
	Channel    string          `json:"channel"`
	Connection ConfigMQ        `json:"connection"`
}

func (q *SetQueue) SendMessage(msg []byte) error {
	return nil
}

func (q *SetQueue) ReceiveMessage(msg []byte) ([]byte, error) {
	return nil, nil
}
