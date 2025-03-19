package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// KafkaExporter 结构
type KafkaExporter struct {
	client          sarama.Client
	admin           sarama.ClusterAdmin
	brokerCount     *prometheus.GaugeVec
	topicPartitions *prometheus.GaugeVec
	groupMembers    *prometheus.GaugeVec
	partitionOffset *prometheus.GaugeVec
	groupOffset     *prometheus.GaugeVec
	groupLag        *prometheus.GaugeVec
}

func NewKafkaExporter(servers []string, config *sarama.Config) (*KafkaExporter, error) {
	client, err := sarama.NewClient(servers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka admin client: %w", err)
	}

	// 定义指标
	brokerCount := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_brokers",
			Help: "Number of Kafka brokers",
		},
		[]string{"cluster"},
	)

	topicPartitions := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_topic_partitions",
			Help: "Number of partitions for a Kafka topic",
		},
		[]string{"topic"},
	)

	groupMembers := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumergroup_members",
			Help: "Number of members in a Kafka consumer group",
		},
		[]string{"group"},
	)

	partitionOffset := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_topic_partition_current_offset",
			Help: "Current offset for a Kafka topic partition",
		},
		[]string{"topic", "partition"},
	)

	groupOffset := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumergroup_current_offset",
			Help: "Current offset for a Kafka consumer group",
		},
		[]string{"group", "topic", "partition"},
	)

	groupLag := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumergroup_lag",
			Help: "Lag for a Kafka consumer group partition",
		},
		[]string{"group", "topic", "partition"},
	)

	// 注册指标
	prometheus.MustRegister(
		brokerCount,
		topicPartitions,
		groupMembers,
		partitionOffset,
		groupOffset,
		groupLag,
	)

	return &KafkaExporter{
		client:          client,
		admin:           admin,
		brokerCount:     brokerCount,
		topicPartitions: topicPartitions,
		groupMembers:    groupMembers,
		partitionOffset: partitionOffset,
		groupOffset:     groupOffset,
		groupLag:        groupLag,
	}, nil
}

func (exporter *KafkaExporter) CollectMetrics() {
	// 更新 Broker 数量
	brokers := exporter.client.Brokers()
	exporter.brokerCount.WithLabelValues("default").Set(float64(len(brokers)))

	// 更新 Topic 分区数
	topics, err := exporter.admin.ListTopics()
	if err != nil {
		log.Printf("Failed to list topics: %v", err)
		return
	}

	for topicName := range topics {
		partitions, err := exporter.client.Partitions(topicName)
		if err != nil {
			log.Printf("Failed to get partitions for topic %s: %v", topicName, err)
			continue
		}
		exporter.topicPartitions.WithLabelValues(topicName).Set(float64(len(partitions)))

		// 获取分区当前偏移量（高水位）
		for _, partition := range partitions {
			offset, err := exporter.client.GetOffset(topicName, partition, sarama.OffsetNewest)
			if err != nil {
				log.Printf("Failed to get offset for topic %s partition %d: %v", topicName, partition, err)
				continue
			}
			exporter.partitionOffset.WithLabelValues(topicName, fmt.Sprintf("%d", partition)).Set(float64(offset))
		}
	}

	// 更新消费者组指标
	groups, err := exporter.admin.ListConsumerGroups()
	if err != nil {
		log.Printf("Failed to list consumer groups: %v", err)
		return
	}

	for groupID := range groups {
		// 获取消费者组详情
		desc, err := exporter.admin.DescribeConsumerGroups([]string{groupID})
		if err != nil || len(desc) == 0 {
			log.Printf("Failed to describe consumer group %s: %v", groupID, err)
			continue
		}

		// 设置组成员数量
		memberCount := len(desc[0].Members)
		exporter.groupMembers.WithLabelValues(groupID).Set(float64(memberCount))

		// 获取消费者组偏移量
		offsets, err := exporter.admin.ListConsumerGroupOffsets(groupID, nil)
		if err != nil {
			log.Printf("Failed to get offsets for group %s: %v", groupID, err)
			continue
		}

		// 遍历每个分区的偏移量
		for topic, partitions := range offsets.Blocks {
			for partition, offsetBlock := range partitions {
				if offsetBlock.Err != sarama.ErrNoError {
					log.Printf("Error in offset block for group %s topic %s partition %d: %v",
						groupID, topic, partition, offsetBlock.Err)
					continue
				}

				// 获取高水位偏移量
				highWatermark, err := exporter.client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					log.Printf("Failed to get high watermark for topic %s partition %d: %v",
						topic, partition, err)
					continue
				}

				// 计算滞后并设置指标
				currentOffset := offsetBlock.Offset
				lag := highWatermark - currentOffset

				partitionStr := fmt.Sprintf("%d", partition)
				exporter.groupOffset.WithLabelValues(groupID, topic, partitionStr).Set(float64(currentOffset))
				exporter.groupLag.WithLabelValues(groupID, topic, partitionStr).Set(float64(lag))
			}
		}
	}
}

func (exporter *KafkaExporter) Start(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		exporter.CollectMetrics()
	}
}

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0

	exporter, err := NewKafkaExporter([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatal("Failed to create Kafka exporter:", err)
	}

	// 启动指标收集
	go exporter.Start(15 * time.Second)

	// 启动 Prometheus HTTP 服务
	http.Handle("/metrics", promhttp.Handler())
	log.Println("Starting Prometheus exporter on :2112")
	if err := http.ListenAndServe(":2112", nil); err != nil {
		log.Fatal("Failed to start HTTP server:", err)
	}
}
