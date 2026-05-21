package com.semantyca.datanest.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.semantyca.datanest.EnvConst;
import com.semantyca.mixpla.dto.queue.metric.MetricEventDTO;
import com.semantyca.mixpla.dto.queue.metric.MetricEventType;
import com.semantyca.mixpla.dto.queue.metric.ProcessType;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class CommandPublisher {
    private static final Logger LOGGER = Logger.getLogger(CommandPublisher.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Inject
    @Channel("commands")
    Emitter<byte[]> commandsEmitter;

    public void publishCommand(String brandName, MetricEventType eventType, ProcessType processType, String code, Map<String, Object> payload) {
        publishCommand(brandName, eventType, processType, code, payload, UUID.randomUUID());
    }

    public void publishCommand(String brandName, MetricEventType eventType, ProcessType processType, String code, Map<String, Object> payload, UUID traceId) {
        try {
            MetricEventDTO event = MetricEventDTO.of(
                    EnvConst.APP_ID,
                    brandName,
                    eventType,
                    processType,
                    traceId,
                    code,
                    payload
            );
            publish(event)
                    .subscribe()
                    .with(
                            v -> LOGGER.debugf("Published command for {}: {}", brandName, code),
                            e -> LOGGER.errorf("Failed to publish command for {}: {}", brandName, e.getMessage())
                    );
        } catch (Exception e) {
            LOGGER.errorf("Error publishing command for {}: {}", brandName, e.getMessage());
        }
    }

    private Uni<Void> publish(MetricEventDTO event) {
        return Uni.createFrom().item(() -> {
                    try {
                        return objectMapper.writeValueAsBytes(event);
                    } catch (Exception e) {
                        LOGGER.error("Failed to serialize command event", e);
                        throw new RuntimeException(e);
                    }
                })
                .invoke(bytes -> commandsEmitter.send(bytes))
                .onFailure().invoke(e -> LOGGER.error("Failed to publish command event", e))
                .onItem().ignore().andContinueWithNull();
    }
}
