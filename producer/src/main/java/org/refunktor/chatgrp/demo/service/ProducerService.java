package org.refunktor.chatgrp.demo.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.refunktor.chatgrp.demo.kafka.Producer;
import org.refunktor.chatgrp.demo.model.OutboundMessage;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProducerService {
    private final Producer kafkaProducer;

    @Scheduled(fixedRate = 1000)
    public void sendKafkaMessage() {
        OutboundMessage message = new OutboundMessage(UUID.randomUUID(), getMessage(), LocalDateTime.now());
        log.info("Sending message: {}", message);
        kafkaProducer.sendMessage(message);
    }

    private String getMessage() {
        var code = new Random().nextInt(0, 5);
        switch (code) {
            case 0:
                return "SUCCESS";
            case 1:
                return "ERROR";
            case 2:
                return "RETRY";
            case 3:
                return "PIZZA";
            case 4:
                return "COKE";
            default:
                throw new RuntimeException("Unexpected!");
        }
    }
}
