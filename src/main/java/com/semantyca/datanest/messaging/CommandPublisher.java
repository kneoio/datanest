package com.semantyca.datanest.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.semantyca.datanest.EnvConst;
import com.semantyca.mixpla.dto.queue.command.CommandDTO;
import com.semantyca.mixpla.dto.queue.command.CommandType;
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

    public void publishCommand(CommandType type, String command, Map<String, Object> payload) {
        publishCommand(type, command, payload, UUID.randomUUID());
    }

    public void publishCommand(CommandType type, String command, Map<String, Object> payload, UUID traceId) {
        try {
            CommandDTO event = CommandDTO.of(EnvConst.APP_ID, type, traceId, command, payload);
            publish(event)
                    .subscribe()
                    .with(
                            v -> LOGGER.debugf("Published command {}: {}", type, command),
                            e -> LOGGER.errorf("Failed to publish command {}: {}", command, e.getMessage())
                    );
        } catch (Exception e) {
            LOGGER.errorf("Error publishing command {}: {}", command, e.getMessage());
        }
    }

    private Uni<Void> publish(CommandDTO event) {
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
