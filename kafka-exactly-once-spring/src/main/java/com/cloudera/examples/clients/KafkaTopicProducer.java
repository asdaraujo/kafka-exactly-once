package com.cloudera.examples.clients;

import com.cloudera.examples.configs.KafkaProducerConfig;
import com.cloudera.examples.generated.OffsetCommitKey;
import com.cloudera.examples.generated.OffsetCommitValue;
import com.cloudera.examples.serde.OffsetCommitSerDe;
import com.cloudera.examples.configs.KafkaConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Service
public class KafkaTopicProducer {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicProducer.class);

	final static private OffsetCommitSerDe SER_DE = new OffsetCommitSerDe();

	// topic -> (partition, buffer)
	final private Map<String, Map<Integer, List<ConsumerRecord<byte[], byte[]>>>> topicBuffers = new HashMap<>();
	final private AtomicInteger bufferSize = new AtomicInteger(0);
	final private ReentrantReadWriteLock flushLock = new ReentrantReadWriteLock();
	private long lastFlushMs = System.currentTimeMillis();

	@Autowired
	private KafkaTemplate<Object, Object> kafkaTemplate;

	@Autowired
	private KafkaProducerConfig kafkaProducerConfig;

	@Autowired
	private KafkaConsumerConfig kafkaConsumerConfig;

	/**
	 * Add a new record to the producer buffer.
	 *
	 * @param record CosumerRecord to be sent to the target topic.
	 */
	public void produce(ConsumerRecord<byte[], byte[]> record) {
		addToBuffer(record);

		if (bufferSize.get() >= kafkaProducerConfig.commitBatchSize) {
			flush();
		}
	}

	/**
	 * Call flush only if the last flush happened more than commitMaxIntervalMs milliseconds ago.
	 */
	public void maybeFlush() {
		if (System.currentTimeMillis() - lastFlushMs > kafkaProducerConfig.commitMaxIntervalMs) {
			flush();
		}
	}

	/**
	 * Flush the data, sending all the buffer content to the target topic. The source offsets are also written
	 * to a topic in the target cluster, within the same transaction as the data.
	 */
	public void flush() {
		try {
			// ensures no other threads are writing to buffers while the flush happens
			flushLock.writeLock().lock();

			if (LOG.isDebugEnabled()) {
				printBuffersToLog();
			}

			if (bufferSize.get() > 0) {
				Map<OffsetCommitKey, OffsetCommitValue> committedOffsets = new HashMap<>();
				kafkaTemplate.executeInTransaction(kafkaTemplate -> {
					List<ListenableFuture<SendResult<Object, Object>>> futures = new LinkedList<>();

					// send messages
					topicBuffers.forEach((topic, partitionBuffers) -> {
						partitionBuffers.forEach((partition, records) -> {
							records.forEach(record -> {
								futures.add(kafkaTemplate.send(kafkaProducerConfig.topic, record.key(), record.value()));
								// keep track of the sent messages offsets
								committedOffsets.put(
										new OffsetCommitKey(kafkaConsumerConfig.groupId(), topic, partition),
										new OffsetCommitValue(record.offset()));
							});
						});
					});

					// send offsets
					committedOffsets.forEach((key, value) -> {
						futures.add(kafkaTemplate.send(kafkaProducerConfig.offsetTopic,
								SER_DE.serializeKey(key),
								SER_DE.serializeValue(value)));
					});

					// ensure all futures complete successfully before commit. An exception will trigger a rollback.
					futures.forEach(f -> {
						try {
							f.get();
						} catch (ExecutionException | InterruptedException e) {
							throw new RuntimeException("Failed to send records and/or offsets", e);
						}
					});

					if (LOG.isInfoEnabled()) {
						printCommittedOffsetsToLog(committedOffsets);
					}

					// update buffers and timestamp
					topicBuffers.clear();
					bufferSize.set(0);
					lastFlushMs = System.currentTimeMillis();
					return true;
				});
			}
		} finally {
			flushLock.writeLock().unlock();
		}
	}

	/**
	 * Discard the content of the specified partition. Called by onLostPartition.
	 *
	 * @param topic
	 * @param partition
	 */
	public void discardPartition(String topic, int partition) {
		if (bufferSize.get() > 0) {
			try {
				flushLock.readLock().lock();

				List<ConsumerRecord<byte[], byte[]>> recordsToDiscard = topicBuffers
						.getOrDefault(topic, new HashMap<>())
						.getOrDefault(partition, null);
				if (recordsToDiscard != null) {
					synchronized (recordsToDiscard) {
						bufferSize.addAndGet(-recordsToDiscard.size());
						recordsToDiscard.clear();
					}
				}
			} finally {
				flushLock.readLock().unlock();
			}
		}
	}

	/**
	 * Add a ConsumerRecord to the buffer.
	 *
	 * @param record ConsumerRecord from the source topic.
	 */
	private void addToBuffer(ConsumerRecord<byte[], byte[]> record) {
		try {
			flushLock.readLock().lock();

			Map<Integer, List<ConsumerRecord<byte[], byte[]>>> partitionBuffers = topicBuffers.getOrDefault(record.topic(), null);
			if (partitionBuffers == null) {
				synchronized (topicBuffers) {
					partitionBuffers = topicBuffers.computeIfAbsent(record.topic(), k -> new HashMap<>());
				}
			}
			List<ConsumerRecord<byte[], byte[]>> buffer = partitionBuffers.getOrDefault(record.partition(), null);
			if (buffer == null) {
				synchronized (partitionBuffers) {
					buffer = partitionBuffers.computeIfAbsent(record.partition(), k -> new LinkedList<>());
				}
			}
			synchronized (buffer) {
				buffer.add(record);
				bufferSize.incrementAndGet();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			flushLock.readLock().unlock();
		}
		maybeFlush();
	}

	/**
	 * Prints buffers' sizes to the log. For debugging purposes only.
	 */
	private void printBuffersToLog() {
		LOG.debug("Buffer size: {}", bufferSize.get());
		topicBuffers.forEach((topic, partitionBuffers) -> {
			LOG.debug("Records in topic [{}]'s buffers:", topic);
			partitionBuffers.forEach((partition, buffer) -> {
				LOG.debug("  - Partition [{}-{}]: {} records", topic, partition, buffer.size());
			});
		});
	}

	/**
	 * Prints committed offsets to the log. For debugging purposes only.
	 */
	private void printCommittedOffsetsToLog(Map<OffsetCommitKey, OffsetCommitValue> committedOffsets) {
		LOG.debug("Committed offsets: {}", committedOffsets.entrySet().stream()
				.map(e -> {
					OffsetCommitKey k = e.getKey();
					return k.getGroup() + ":" + k.getTopic() + "-" + k.getPartition() + ":" + e.getValue().getOffset();
				})
				.sorted()
				.collect(Collectors.joining(",")));
	}

}
