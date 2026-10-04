using Consumer.Models;
using Elastic.Clients.Elasticsearch;


namespace Consumer.Services
{
    public class ElasticService
    {
        private readonly ElasticsearchClient _client;
        private readonly string _indexName;

        public ElasticService(string elasticUri, string indexName)
        {
            var settings = new ElasticsearchClientSettings(new Uri(elasticUri))
                .DefaultIndex(indexName);

            _client = new ElasticsearchClient(settings);
            _indexName = indexName;
        }

        public async Task EnsureIndexWithMappingAsync()
        {
            var existsResponse = await _client.Indices.ExistsAsync(_indexName);
            if (!existsResponse.Exists)
            {
                var createResponse = await _client.Indices.CreateAsync(_indexName, c => c
                    .Mappings(m => m
                        .Properties<SensorRecord>(p => p
                            .Date(n => n.Timestamp, d => d.Format("strict_date_optional_time||epoch_millis"))
                            .Keyword(n => n.DeviceId)
                            .Keyword(n => n.MetricName)
                            .DoubleNumber(n => n.Value)
                            .Keyword(n => n.Status)
                        )
                    )
                );

                if (createResponse.IsValidResponse)
                {
                    Console.WriteLine($"[+] Index '{_indexName}' created successfully with custom mapping.");
                }
                else
                {
                    Console.WriteLine($"[!] Error creating index: {createResponse.DebugInformation}");
                }
            }
            else
            {
                Console.WriteLine($"[i] Index '{_indexName}' already exists.");
            }
        }

        public async Task<bool> IndexRecordAsync(SensorRecord record)
        {
            var response = await _client.IndexAsync(record, idx => idx.Index(_indexName));
            if (response.IsValidResponse)
            {
                Console.WriteLine($"--> Successfully indexed to Elasticsearch (ID: {response.Id})");
                return true;
            }
            else
            {
                Console.WriteLine($"[!] Failed to index to Elasticsearch: {response.DebugInformation}");
                return false;
            }
        }
    }
}
