package org.refunktor.chatgrp.demo.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.refunktor.chatgrp.demo.model.OutboundMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(topics = "hackaton")
@Testcontainers
@ContextConfiguration(initializers = ProducerIT.Initializer.class)
class ProducerIT {

    @Container
    private static final KafkaContainer kafkaContainer = new KafkaContainer();

    @Autowired
    private Producer kafkaProducer;

    private static BlockingQueue<String> messages;

    @BeforeAll
    public static void setUp() throws Exception {
        messages = new LinkedBlockingQueue<>();
    }

    @AfterEach
    public void tearDown() throws Exception {
        messages.clear();
    }

    @Test
    public void testSendMessage() throws Exception {
        // Arrange
        UUID uuid = UUID.randomUUID();
        String text = "Hello, Kafka!";
        LocalDateTime timestamp = LocalDateTime.now();
        OutboundMessage message = new OutboundMessage(uuid, text, timestamp);

        // Act
        kafkaProducer.sendMessage(message);

        // Assert
        String receivedMessage = messages.poll(10, TimeUnit.SECONDS);
        assertThat(receivedMessage).isEqualTo(message.toString());
    }

    static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            String brokers = kafkaContainer.getBootstrapServers();
            TestPropertyValues.of("spring.kafka.bootstrap-servers=" + brokers).applyTo(applicationContext);
        }
    }

//    @Autowired
//    public void setKafkaListener(BlockingQueue<String> messages) {
//        ProducerIT.messages = messages;
//    }

    @org.springframework.kafka.annotation.KafkaListener(topics = "my-topic", groupId = "test-group")
    public void listen(String message) {
        messages.add(message);
    }
}