package org.refunktor.chatgrp.demo.model;

import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.time.LocalDateTime;
import java.util.UUID;

@RequiredArgsConstructor
@Value
public class OutboundMessage {
    private final UUID uuid;
    private final String text;
    private final LocalDateTime timestamp;
}
