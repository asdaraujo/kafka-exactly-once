package com.cloudera.examples.configs;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka.producer")
public class KafkaOffsetConsumerConfig {

	private String offsetTopic;
	private Map<String, String> properties;

	private Map<String, Object> getDefaultProps() {
		Map<String, Object> props = new HashMap<String, Object>(properties);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
		return props;
	}

	@Bean
	public ConsumerFactory<byte[], byte[]> offsetConsumerFactory() {
		Map<String, Object> props = getDefaultProps();
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConsumerFactory<byte[], byte[]> offsetNonTransactionalConsumerFactory() {
		Map<String, Object> props = getDefaultProps();
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
		return new DefaultKafkaConsumerFactory<>(props);
	}

}
