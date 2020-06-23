package conf

// ServiceConfig 服务配置
type ServiceConfig struct {
	HTTPEndpoint string          `json:"http_endpoint"`
	GRPCEndpoint string          `json:"grpc_endpoint"`
	Pipeline     *PipelineConfig `json:"pipeline"`
}

// PipelineConfig 处理管道配置
type PipelineConfig struct {
	Kafka   *KafkaConfig   `json:"kafka"`
	Minio   *MinioConfig   `json:"minio"`
	MySQL   *MySQLConfig   `json:"mysql"`
	Indexer *IndexerConfig `json:"indexer"`
}

// KafkaConfig Kafka连接配置
type KafkaConfig struct {
	Brokers      []string `json:"brokers"`
	Topic        []string `json:"topic"`
	Version      string   `json:"version"`
	ConsumeGroup string   `json:"consume_group"`
	FromOldest   bool     `json:"from_oldest"`
}

// MinioConfig Minio连接配置
type MinioConfig struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	UseSSL    bool   `json:"use_ssl"`
	Bucket    string `json:"bucket"`
	Root      string `json:"root"`
}

// MySQLConfig MySQL连接配置
type MySQLConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

// IndexerConfig 索引器配置
type IndexerConfig struct {
	DumpPath string `json:"dump_path"`
}
