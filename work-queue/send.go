package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	//连接我们之前启动的 mq-server
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	//创建一个通道，其中存放了大多数用于完成任务的 API
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	//声明要发送的消息queue队列
	q, err := ch.QueueDeclare(
		"work-queue", // name 队列的名称
		true,         // durable 持久化，重启后队列是否继续存在
		false,        // delete when unused 没有消费时自动删除
		false,        // exclusive 独占，仅供一个连接实用，连接关闭时队列也会删除
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	//要发送的消息的body体
	body := bodyFrom(os.Args)

	//发送消息
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, //发送的消息持久化，本质是落盘，不能完全保证不丢失
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	failOnError(err, "Failed to publish a message")
	log.Printf(" [x] Sent %s", body)
}

//打印错误日志
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

//从输入中获取参数，类似 “msg..”
//几个 . 代表消费时间消耗几秒，来模拟消息需要耗费很长时间的情况
func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}
