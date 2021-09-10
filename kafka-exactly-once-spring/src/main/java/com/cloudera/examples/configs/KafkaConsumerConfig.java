package com.cloudera.examples.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
@Primary
@ConfigurationProperties(prefix = "kafka.consumer")
public class KafkaConsumerConfig {

	public String topic;
	private Map<String, String> properties;

	@Bean
	public ConsumerFactory<byte[], byte[]> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(new HashMap<>(properties));
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<byte[], byte[]> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<byte[], byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setAutoStartup(true);
		factory.setBatchListener(false);
		factory.setConcurrency(5);
		factory.setConsumerFactory(consumerFactory());
		factory.getContainerProperties().setIdleEventInterval(1000L);
		factory.getContainerProperties().setAckMode(AckMode.MANUAL);
		return factory;
	}

	public String groupId() {
		return properties.get("group.id");
	}
}
