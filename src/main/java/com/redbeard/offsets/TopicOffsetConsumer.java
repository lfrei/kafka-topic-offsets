package com.redbeard.offsets;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Service
public class TopicOffsetConsumer {

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    @PostConstruct
    private void initConsumer() {
        if (kafkaConsumer == null) {
            final var properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "noop-consumer");

            kafkaConsumer = new KafkaConsumer<>(properties);
        }
    }

    public Long getOffset(String topic) {
        List<TopicPartition> partitions = getPartitions(topic);

        return kafkaConsumer.endOffsets(partitions)
                .values().stream()
                .reduce(0L, Long::sum);
    }

    private List<TopicPartition> getPartitions(String topic) {
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);

        if (partitionInfos == null) {
            return List.of();
        }

        return partitionInfos.stream()
                .map(this::createTopicPartition)
                .collect(Collectors.toList());
    }

    private TopicPartition createTopicPartition(PartitionInfo partitionInfo) {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }
}
