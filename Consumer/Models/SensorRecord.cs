namespace Consumer.Models
{
    public class SensorRecord
    {
        public DateTime Timestamp { get; set; }
        public string DeviceId { get; set; } = string.Empty;
        public string MetricName { get; set; } = string.Empty;
        public double Value { get; set; }
        public string Status { get; set; } = string.Empty;
    }
}