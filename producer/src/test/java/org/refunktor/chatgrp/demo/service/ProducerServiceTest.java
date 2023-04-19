package org.refunktor.chatgrp.demo.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.refunktor.chatgrp.demo.kafka.Producer;
import org.refunktor.chatgrp.demo.model.OutboundMessage;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ProducerServiceTest {

    @Mock
    private Producer producer;

    @InjectMocks
    private ProducerService producerService;

    @Test
    public void testSendKafkaMessage() {
        OutboundMessage message = new OutboundMessage(UUID.randomUUID(), "SUCCESS", LocalDateTime.now());
        when(producer.sendMessage(message)).thenReturn(true);

        producerService.sendKafkaMessage();

        Mockito.verify(producer, times(1)).sendMessage(message);
    }

    @Test
    public void testGetMessage() {
        String message = producerService.getMessage();
        assertThat(message, anyOf(equalTo("SUCCESS"), equalTo("ERROR"), equalTo("RETRY"), equalTo("PIZZA"), equalTo("COKE")));
    }
}