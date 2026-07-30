package com.semantyca.datanest.messaging;

import com.semantyca.core.dto.queue.command.CommandDTO;
import com.semantyca.core.messaging.AbstractCommandPublisher;
import com.semantyca.datanest.EnvConst;
import com.semantyca.mixpla.dto.queue.command.CommandType;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class CommandPublisher extends AbstractCommandPublisher {
    private static final Logger LOGGER = Logger.getLogger(CommandPublisher.class);

    @Inject
    @Channel("commands")
    Emitter<byte[]> commandsEmitter;

    @Override
    protected Emitter<byte[]> getEmitter() {
        return commandsEmitter;
    }

    /**
     * Publishes a command and returns the traceId that starts this flow in datanest.
     * Callers that also emit a metric should reuse the returned id.
     */
    public UUID publishCommand(CommandType type, String command, Map<String, Object> payload) {
        return publishCommand(type, command, payload, UUID.randomUUID());
    }

    public UUID publishCommand(CommandType type, String command, Map<String, Object> payload, UUID traceId) {
        UUID id = traceId != null ? traceId : UUID.randomUUID();
        try {
            CommandDTO event = CommandDTO.of(EnvConst.APP_ID, type, id, command, payload);
            publishEvent(event, "jesoos");
            if (CommandType.FLOW_RESTART.equals(type)) {
                publishEvent(event, "aivox");
            }
            return id;
        } catch (Exception e) {
            LOGGER.errorf(e, "Error publishing command type=%s command=%s traceId=%s", type, command, id);
            return id;
        }
    }

    private void publishEvent(CommandDTO event, String routingKey) {
        LOGGER.infof("Publishing command type=%s command=%s traceId=%s routingKey=%s payload=%s",
                event.type(), event.command(), event.traceId(), routingKey, event.payload());
        Uni.createFrom().item(() -> {
                    try {
                        return objectMapper.writeValueAsBytes(event);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to serialize command event", e);
                    }
                })
                .invoke(bytes -> commandsEmitter.send(Message.of(bytes)
                        .addMetadata(OutgoingRabbitMQMetadata.builder()
                                .withRoutingKey(routingKey)
                                .build())))
                .onItem().ignore().andContinueWithNull()
                .subscribe().with(
                        ignored -> LOGGER.infof("Command published OK type=%s traceId=%s routingKey=%s",
                                event.type(), event.traceId(), routingKey),
                        failure -> LOGGER.errorf(failure,
                                "Failed to publish command type=%s traceId=%s routingKey=%s",
                                event.type(), event.traceId(), routingKey)
                );
    }
}
