using Consumer.Services;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Consumer.Models;
using System.Text;
using System.Text.Json;

namespace Consumer.Services
{
    public class RabbitMQConsumerService
    {
        private readonly string _rabbitHost;
        private readonly string _queueName;
        private readonly ElasticService _elasticService;

        public RabbitMQConsumerService(string rabbitHost, string queueName, ElasticService elasticService)
        {
            _rabbitHost = rabbitHost;
            _queueName = queueName;
            _elasticService = elasticService;
        }

        public void StartConsuming()
        {
            var factory = new ConnectionFactory() { HostName = _rabbitHost };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.QueueDeclare(queue: _queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

            Console.WriteLine(" [*] Waiting for messages from RabbitMQ. To exit press CTRL+C");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += async (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();
                    var jsonMessage = Encoding.UTF8.GetString(body);

                    Console.WriteLine($"[x] Received: {jsonMessage}");

                    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                    var record = JsonSerializer.Deserialize<SensorRecord>(jsonMessage, options);

                    if (record != null)
                    {
                        bool isIndexed = await _elasticService.IndexRecordAsync(record);

                        if (isIndexed)
                        {
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        }
                        else
                        {
                            channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[!] Error processing message: {ex.Message}");
                    channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
                }
            };

            channel.BasicConsume(queue: _queueName, autoAck: false, consumer: consumer);

            var tcs = new TaskCompletionSource();
            tcs.Task.Wait();
        }
    }
}