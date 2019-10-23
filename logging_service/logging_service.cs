using System;
using System.Text;
using System.IO;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace logging_service
{
    class logging_service
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName="localhost" };

            var queueName = "logging_queue";
            var exchangeName = "order_exchange";
            var routingKey = "create_order";

            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: queueName,
                                        durable: false,
                                        exclusive: false,
                                        autoDelete: false,
                                        arguments: null);
                    
                    channel.QueueBind(queue: queueName,
                                    exchange: exchangeName,
                                    routingKey: routingKey);
                    
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) => {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);

                        var newLog = "Log: " + message + "\n";
                        File.AppendAllText("log.txt", newLog);
                    };
                    channel.BasicConsume(queue: queueName,
                                        
                                        autoAck: true,
                                        consumer: consumer);

                    Console.ReadLine();
                }
            }
        }
    }
}
