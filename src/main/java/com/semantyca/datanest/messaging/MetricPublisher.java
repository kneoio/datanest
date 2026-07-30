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

import java.util.List;
import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class MetricPublisher {
    private static final Logger LOGGER = Logger.getLogger(MetricPublisher.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Inject
    @Channel("metrics")
    Emitter<byte[]> metricsEmitter;

    public void publishMetric(String brandName, MetricEventType eventType, ProcessType processType, String code, Map<String, Object> payload) {
        publishMetric(brandName, eventType, processType, code, payload, UUID.randomUUID());
    }

    public void publishMetric(String brandName, MetricEventType eventType, ProcessType processType, String code, Map<String, Object> payload, UUID traceId) {
        try {
            MetricEventDTO event = MetricEventDTO.of(
                    EnvConst.APP_ID,
                    brandName,
                    eventType,
                    processType,
                    traceId,
                    List.of(),
                    code,
                    payload
            );
            LOGGER.infof("Publishing metric brand=%s type=%s code=%s traceId=%s", brandName, eventType, code, traceId);
            publish(event)
                    .subscribe()
                    .with(
                            v -> LOGGER.infof("Metric published OK brand=%s type=%s code=%s traceId=%s", brandName, eventType, code, traceId),
                            e -> LOGGER.errorf(e, "Failed to publish metric brand=%s type=%s code=%s", brandName, eventType, code)
                    );
        } catch (Exception e) {
            LOGGER.errorf(e, "Error publishing metric brand=%s type=%s code=%s", brandName, eventType, code);
        }
    }

    private Uni<Void> publish(MetricEventDTO event) {
        return Uni.createFrom().item(() -> {
                    try {
                        return objectMapper.writeValueAsBytes(event);
                    } catch (Exception e) {
                        LOGGER.error("Failed to serialize metric event", e);
                        throw new RuntimeException(e);
                    }
                })
                .invoke(bytes -> metricsEmitter.send(bytes))
                .onFailure().invoke(e -> LOGGER.error("Failed to publish metric event", e))
                .onItem().ignore().andContinueWithNull();
    }
}