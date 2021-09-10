package com.cloudera.examples.configs;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
@Primary
@ConfigurationProperties(prefix = "kafka.producer")
public class KafkaProducerConfig {

	public String topic;
	public String offsetTopic;
	public int commitBatchSize;
	public long commitMaxIntervalMs;
	private String transactionIdPrefix;
	private Map<String, String> properties;

	@Bean
	public ProducerFactory<Object, Object> producerFactory() {
		DefaultKafkaProducerFactory<Object, Object> factory = new DefaultKafkaProducerFactory<>(new HashMap<>(properties));
		factory.setTransactionIdPrefix(transactionIdPrefix);
		return factory;
	}

	@Bean
	public KafkaTemplate<Object, Object> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public KafkaTransactionManager<?,?> kafkaTransactionManager(ProducerFactory<Object, Object> producerFactory) {
		return new KafkaTransactionManager<>(producerFactory());
	}

}
