package conf

// PipelineConfig 处理管道配置
type PipelineConfig struct {
	Kafka *KafkaConfig `json:"kafka"`
	Minio *MinioConfig `json:"minio"`
}

// KafkaConfig kafka连接配置
type KafkaConfig struct {
	Brokers      []string `json:"brokers"`
	Topic        []string `json:"topic"`
	Version      string   `json:"version"`
	ConsumeGroup string   `json:"consume_group"`
	FromOldest   bool     `json:"from_oldest"`
}

// MinioConfig minio连接配置
type MinioConfig struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	UseSSL    bool   `json:"use_ssl"`
	Bucket    string `json:"bucket"`
	Root      string `json:"root"`
}
