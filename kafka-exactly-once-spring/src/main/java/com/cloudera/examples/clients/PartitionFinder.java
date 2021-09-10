package com.cloudera.examples.clients;

import com.cloudera.examples.serde.OffsetCommitSerDe;
import com.cloudera.examples.configs.KafkaConsumerConfig;
import com.cloudera.examples.configs.KafkaProducerConfig;
import com.cloudera.examples.generated.OffsetCommitKey;
import com.cloudera.examples.generated.OffsetCommitValue;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class PartitionFinder {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionFinder.class);

    @Autowired
    private KafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private KafkaConsumerConfig kafkaConsumerConfig;

    final static private OffsetCommitSerDe SER_DE = new OffsetCommitSerDe();
    private final ConsumerFactory<byte[], byte[]> offsetConsumerFactory;
    private final ConsumerFactory<byte[], byte[]> offsetNonTransactionalConsumerFactory;

    public PartitionFinder(ConsumerFactory<byte[], byte[]> offsetConsumerFactory,
                           ConsumerFactory<byte[], byte[]> offsetNonTransactionalConsumerFactory) {
        this.offsetConsumerFactory = offsetConsumerFactory;
        this.offsetNonTransactionalConsumerFactory = offsetNonTransactionalConsumerFactory;
    }

    /**
     * Return the last committed offset for one or more partitions.
     *
     * @param partitions The collection of partitions to look up.
     * @return A map of partitions to their last committed offset.
     */
    public Map<TopicPartition, Long> getCommittedOffsets(Collection<TopicPartition> partitions) {
        if (kafkaProducerConfig.offsetTopic == null) {
            return null;
        }

        try (Consumer<byte[], byte[]> consumer = getOffsetConsumer()) {
            // Get LEOs of the offset topic to ensure we always read until the end. Without this we can incur in
            // duplicated messages being sent to the target topic.
            Map<TopicPartition, Long> endOffsets = getAssignmentEndOffsets(consumer);

            Map<TopicPartition, Long> committedOffsets = new HashMap<>();
            int totalOffsets = 0;
            int selectedOffsets = 0;
            int waitMs = 100;
            double exponentialWaitBackoff = 1.5;
            int maxWaits = 30;
            int waits = 0;
            boolean isEndOfTopic = false;
            while (!isEndOfTopic) {
                // Consume offsets. If poll times out, increase the timeout until we succeed. Otherwise, fails after
                // a set number of attempts. This is to ensure that we don't take a poll timeout as an indication
                // that we arrived at the end of the topic.
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(waitMs));
                if (records.count() == 0) {
                    LOG.warn("No committed offsets after waiting {} ms. Backing off by a factor of {} for retry #{}",
                            waitMs, exponentialWaitBackoff, ++waits);
                    waitMs = (int) (waitMs * exponentialWaitBackoff);
                } else {
                    waits = 0;
                }

                // Deserialize the offset data and keep track of the ones we're interested in
                totalOffsets += records.count();
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    try {
                        OffsetCommitKey key = getOffsetCommitKey(record);
                        TopicPartition tp = new TopicPartition(key.getTopic(), key.getPartition());
                        if (key.getGroup().equals(kafkaConsumerConfig.groupId())
                                && partitions.contains(tp)) {
                            OffsetCommitValue value = getOffsetCommitValue(record);
                            selectedOffsets += 1;
                            committedOffsets.put(tp, value.getOffset());
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Read from offset topic: TopicPartition {} => Offset {}", tp, value.getOffset());
                            }
                        }
                    } catch (RuntimeException e) {
                        LOG.warn("Committed offset deserialization exception: topic={}, partition={}, offset={}",
                                record.topic(), record.partition(), record.offset());
                    }
                }

                // Check if we reached LEO for all offset topic partitions
                isEndOfTopic = true;
                for (Map.Entry<TopicPartition, Long> eo : endOffsets.entrySet()) {
                    TopicPartition tp = eo.getKey();
                    long endOffset = eo.getValue();
                    if (consumer.position(tp) < endOffset) {
                        isEndOfTopic = false;
                        break;
                    }
                }

                // If we already waited more than maxWaits and haven't reached the end of the topic, throws an exception
                if (waits > maxWaits) {
                    throw new RuntimeException("Timed out while trying to fetch committed offsets.");
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Processed {} records from the offset topic. " +
                                "Found {} committed offsets for the requested groupId: {}. " +
                                "Latest committed offsets are: {}",
                        totalOffsets, selectedOffsets, kafkaConsumerConfig.groupId(),
                        committedOffsets.entrySet().stream()
                                .map(e -> e.getKey().toString() + ":" + e.getValue())
                                .sorted().collect(Collectors.joining(",")));
            }

            return committedOffsets;
        }
    }

    /**
     * Create a consumer to read the committed offsets.
     *
     * @return A KafkaConsumer ready to start consuming offsets.
     */
    private Consumer<byte[], byte[]> getOffsetConsumer() {
        Consumer<byte[], byte[]> consumer = offsetConsumerFactory.createConsumer("offsetReader");
        List<TopicPartition> partitions = consumer
                .partitionsFor(kafkaProducerConfig.offsetTopic).stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .collect(Collectors.toList());
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        return consumer;
    }

    /**
     * Find the Log End Offsets (LEOs) for the assignment of the specified consumer.
     * Use a non-transactional consumer to ensure we get the LEO and not the LSO (Log Stable Offset).
     *
     * @param consumer The KafkaConsumer. Must have an assignment.
     * @return A map of TopicPartitions to their LEO.
     */
    private Map<TopicPartition, Long> getAssignmentEndOffsets(Consumer<?, ?> consumer) {
        Map<TopicPartition, Long> endOffsets;
        try (Consumer<byte[], byte[]> nonXactConsumer = offsetNonTransactionalConsumerFactory.createConsumer()) {
            endOffsets = nonXactConsumer.endOffsets(consumer.assignment());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Assignment LEOs: {}",
                    endOffsets.entrySet().stream()
                            .map(e -> e.getKey().toString() + ":" + e.getValue())
                            .sorted().collect(Collectors.joining(",")));
        }
        return endOffsets;
    }

    /**
     * Return an OffsetCommitKey for the specified ConsumerRecord.
     *
     * @param record A ConsumerRecord.
     * @return An OffsetCommitKey instance.
     */
    private OffsetCommitKey getOffsetCommitKey(ConsumerRecord<byte[], byte[]> record) {
        return SER_DE.deserializeKey(record.key());
    }

    /**
     * Return an OffsetCommitValue for the specified ConsumerRecord.
     *
     * @param record A ConsumerRecord.
     * @return An OffsetCommitValue instance.
     */
    private OffsetCommitValue getOffsetCommitValue(ConsumerRecord<byte[], byte[]> record) {
        return SER_DE.deserializeValue(record.value());
    }

}
