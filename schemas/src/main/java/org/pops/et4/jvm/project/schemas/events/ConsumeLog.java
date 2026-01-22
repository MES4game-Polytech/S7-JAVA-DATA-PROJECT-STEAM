package org.pops.et4.jvm.project.schemas.events;

import java.time.Instant;

public record ConsumeLog<T>(
        String consumerId,
        Instant consumeDate,
        String key,
        T event
) {}
