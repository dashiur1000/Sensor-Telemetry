using Consumer.Services;

var elasticUri = Environment.GetEnvironmentVariable("ELASTIC_URI") ?? "http://elasticsearch:9200";
var rabbitHost = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "rabbit"; 
string queueName = "csv_rows_queue";
string indexName = "sensor-telemetry-index";

var elasticService = new ElasticService(elasticUri, indexName);
await elasticService.EnsureIndexWithMappingAsync();

var rabbitConsumer = new RabbitMQConsumerService(rabbitHost, queueName, elasticService);
rabbitConsumer.StartConsuming();