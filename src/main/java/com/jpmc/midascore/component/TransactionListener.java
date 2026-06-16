package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Listens to the configured Kafka topic and turns each record into a Transaction object.
 *
 * The listener does not apply business logic yet; its job is to prove that Midas Core
 * can receive and deserialize Kafka messages from the configured topic.
 */
@Component
public class TransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    /**
     * Spring Kafka resolves the topic name from application.yml so the listener stays
     * configurable instead of hard-coding the topic in source code.
     */
    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        // The message is already deserialized into the domain type, so we only log it here.
        logger.info("Received transaction from Kafka: {}", transaction.toString());
    }
}