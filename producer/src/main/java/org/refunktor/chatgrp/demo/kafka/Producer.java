package org.refunktor.chatgrp.demo.kafka;

import org.refunktor.chatgrp.demo.model.OutboundMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    private static final String TOPIC = "hackaton";

    @Autowired
    private KafkaTemplate<String, OutboundMessage> kafkaTemplate;

    public void sendMessage(OutboundMessage message) {
        kafkaTemplate.send(TOPIC, message.getUuid().toString(), message);
    }
}
