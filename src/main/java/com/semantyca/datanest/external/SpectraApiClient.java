package com.semantyca.datanest.external;

import com.semantyca.datanest.config.DatanestConfig;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

@ApplicationScoped
public class SpectraApiClient {

    @Inject
    DatanestConfig config;

    @Inject
    Vertx vertx;

    private WebClient webClient;

    @PostConstruct
    void init() {
        this.webClient = WebClient.create(vertx);
    }

    public Uni<Void> analyze(UUID soundFragmentId) {
        JsonObject body = new JsonObject().put("soundFragmentId", soundFragmentId.toString());

        return webClient
                .postAbs(config.getSpectraUrl() + "/analyze")
                .putHeader("Content-Type", "application/json")
                .timeout(5000)
                .sendJsonObject(body)
                .onItem().invoke(response -> {
                    if (response.statusCode() != 202) {
                        throw new RuntimeException("Spectra analyze error: " +
                                response.statusCode() + " - " + response.bodyAsString());
                    }
                })
                .replaceWithVoid();
    }
}
