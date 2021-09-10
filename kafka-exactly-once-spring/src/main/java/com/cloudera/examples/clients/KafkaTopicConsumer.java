package com.cloudera.examples.clients;

import com.cloudera.examples.configs.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@EnableConfigurationProperties({KafkaConsumerConfig.class})
public class KafkaTopicConsumer {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicConsumer.class);

	@Autowired
	KafkaTopicProducer kafkaTopicProducer;

	@Autowired
	KafkaConsumerConfig kafkaConsumerConfig;

	@Autowired
	PartitionFinder finder;

	@Component
	private class KafkaListener implements ConsumerAwareRebalanceListener {
		/**
		 * Send each consumed record to the producer.
		 *
		 * @param record Kafka record read from source topic.
		 */
		public void consumeMessage(ConsumerRecord<byte[], byte[]> record) {
			kafkaTopicProducer.produce(record);
		}

		/**
		 * Partition is being gracefully revoked from this client, so we commit the data read so far before letting it
		 * go.
		 *
		 * @param consumer
		 * @param partitions
		 */
		@Override
		public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
			kafkaTopicProducer.flush();
			if (LOG.isDebugEnabled()) {
				LOG.debug("onPartitionsRevokedAfterCommit: {}",
						partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")));
			}
		}

		/**
		 * In a "partition lost" event, the lost partition may actually already being consumed by another client with
		 * the same group id. In this case, we simply discard the partition data to avoid duplications.
		 *
		 * @param consumer
		 * @param partitions
		 */
		@Override
		public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
			partitions.forEach(tp -> kafkaTopicProducer.discardPartition(tp.topic(), tp.partition()));
			if (LOG.isDebugEnabled()) {
				LOG.debug("onPartitionsLost: {}",
						partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")));
			}
		}

		/**
		 * Upon the assignment of a new partition to the listener, fetches the latest offset from the offset topic
		 * so that we can start reading from the correct committed offset.
		 *
		 * @param consumer
		 * @param partitions
		 */
		@Override
		public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
			Map<TopicPartition, Long> offsets = finder.getCommittedOffsets(partitions);
			for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
				consumer.seek(entry.getKey(), entry.getValue() + 1);
			}
			if (LOG.isDebugEnabled()) {
				LOG.debug("onPartitionsAssigned: {}, offsets: {}",
						partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")),
						offsets.entrySet().stream().map(entry -> entry.getKey().toString() + ":" + entry.getValue())
								.collect(Collectors.joining(",")));
			}
		}
	}

	@Bean
	ConcurrentMessageListenerContainer<byte[], byte[]> kafkaSourceListener(PartitionFinder finder,
																		   ConcurrentKafkaListenerContainerFactory<byte[], byte[]> factory,
																		   KafkaListener listener) throws Exception {
		MethodKafkaListenerEndpoint<byte[], byte[]> endpoint = endpoint(finder, listener);
		ConcurrentMessageListenerContainer<byte[], byte[]> container = factory.createListenerContainer(endpoint);
		container.getContainerProperties().setGroupId(kafkaConsumerConfig.groupId());
		container.getContainerProperties().setConsumerRebalanceListener(listener);
		return container;
	}

	MethodKafkaListenerEndpoint<byte[], byte[]> endpoint(PartitionFinder finder,
														 KafkaListener listener) throws NoSuchMethodException {
		MethodKafkaListenerEndpoint<byte[], byte[]> endpoint = new MethodKafkaListenerEndpoint<>();
		endpoint.setBean(listener);
		endpoint.setMethod(KafkaListener.class.getDeclaredMethod("consumeMessage", ConsumerRecord.class));
		endpoint.setTopics(kafkaConsumerConfig.topic);
		endpoint.setMessageHandlerMethodFactory(methodFactory());
		return endpoint;
	}

	DefaultMessageHandlerMethodFactory methodFactory() {
		return new DefaultMessageHandlerMethodFactory();
	}

	/**
	 * Ensures that latest data in the buffer is sent in a timely fashion even if the stream in the source topic stops.
	 *
	 * @param event Container event.
	 */
	@EventListener(condition = "event.listenerId.startsWith('kafkaSourceListener')")
	public void idleEventHandler(ListenerContainerIdleEvent event) {
		kafkaTopicProducer.maybeFlush();
	}

}