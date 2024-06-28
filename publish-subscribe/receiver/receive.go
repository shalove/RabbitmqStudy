package main

import (
	"bytes"
	"log"
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

	//声明要消费的消息queue队列，声明队列是幂等的只有当队列不存在时才会创建
	q, err := ch.QueueDeclare(
		"",    // name， 不填的话会随机生成
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//队列绑定
	err = ch.QueueBind(
		q.Name, // queue name
		"",     // routing key
		"logs", // exchange
		false,
		nil,
	)

	//从队列获取消费信息
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	//读取消费的信息并打印出来
	//传过来的消息有几个 "." 就sleep几秒，来模拟处理消息需要消耗很多时间
	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done")
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}

//打印错误日志
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
