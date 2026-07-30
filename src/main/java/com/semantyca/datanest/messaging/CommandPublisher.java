package com.semantyca.datanest.messaging;

import com.semantyca.core.dto.queue.command.CommandDTO;
import com.semantyca.core.messaging.AbstractCommandPublisher;
import com.semantyca.datanest.EnvConst;
import com.semantyca.mixpla.dto.queue.command.CommandType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
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
            LOGGER.infof("Publishing command type=%s command=%s traceId=%s payload=%s", type, command, id, payload);
            publishEvent(event);
            return id;
        } catch (Exception e) {
            LOGGER.errorf(e, "Error publishing command type=%s command=%s traceId=%s", type, command, id);
            return id;
        }
    }
}
