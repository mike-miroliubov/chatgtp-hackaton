package org.refunktor.chatgrp.demo.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.refunktor.chatgrp.demo.model.OutboundMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest
@EmbeddedKafka(topics = "hackaton")
@Testcontainers
//@ContextConfiguration(initializers = ProducerIT.Initializer.class)
class ProducerServiceIT {
    @Container
    static final KafkaContainer kafka = new KafkaContainer().withEmbeddedZookeeper()
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    @Autowired
    ProducerService producerService;

    @Autowired
    KafkaTemplate<String, OutboundMessage> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker kafkaEmbedded;

    @BeforeEach
    void setUp() {
        kafkaTemplate.setDefaultTopic("hackaton");
    }

    @Test
    void testSendKafkaMessage() throws InterruptedException {
        // Given
        final int numOfMessages = 5;
        final CountDownLatch latch = new CountDownLatch(numOfMessages);

        Consumer<String, OutboundMessage> consumer = createConsumer(latch);

        // When
        producerService.sendKafkaMessage();

        // Then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private Consumer<String, OutboundMessage> createConsumer(CountDownLatch latch) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", kafkaEmbedded);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ConsumerFactory<String, OutboundMessage> consumerFactory = new DefaultKafkaConsumerFactory<>(
                consumerProps,
                new StringDeserializer(),
                new JsonDeserializer<>(OutboundMessage.class));

        Consumer<String, OutboundMessage> consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton("hackaton"));

        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                ConsumerRecords<String, OutboundMessage> records = KafkaTestUtils.getRecords(consumer, 1000);
                records.forEach(record -> {
                    log.info("Received message: {}", record.value());
                    latch.countDown();
                });
            }
        });

        return consumer;
    }

}