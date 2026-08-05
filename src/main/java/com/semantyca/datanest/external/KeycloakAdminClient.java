package com.semantyca.datanest.external;

import com.semantyca.datanest.config.DatanestConfig;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * Realm user provisioning for the OTP login, following the same principle as jesoos'
 * KeycloakAuthService: a login code is only worth sending to an email that has a Keycloak account,
 * so one is created on first sight.
 */
@ApplicationScoped
public class KeycloakAdminClient {
    private static final Logger LOGGER = Logger.getLogger(KeycloakAdminClient.class);

    @Inject
    DatanestConfig config;

    @Inject
    Vertx vertx;

    private WebClient webClient;

    @PostConstruct
    void init() {
        this.webClient = WebClient.create(vertx);
    }

    public boolean isConfigured() {
        return config.getKeycloakUrl().isPresent()
                && config.getKeycloakClientId().isPresent()
                && config.getKeycloakClientSecret().isPresent();
    }

    public Uni<String> findOrCreateUser(String email) {
        String normalizedEmail = email.trim().toLowerCase();
        return getAdminToken()
                .chain(adminToken -> findUserId(normalizedEmail, adminToken)
                        .chain(userId -> userId != null
                                ? Uni.createFrom().item(userId)
                                : createUser(normalizedEmail, adminToken)));
    }

    private Uni<String> getAdminToken() {
        MultiMap form = MultiMap.caseInsensitiveMultiMap()
                .add("grant_type", "client_credentials")
                .add("client_id", config.getKeycloakClientId().orElseThrow())
                .add("client_secret", config.getKeycloakClientSecret().orElseThrow());

        return webClient.postAbs(realmBase() + "/protocol/openid-connect/token")
                .timeout(10000)
                .sendForm(form)
                .map(resp -> {
                    if (resp.statusCode() != 200) {
                        throw new KeycloakAdminException("Admin token request failed: HTTP " + resp.statusCode());
                    }
                    return resp.bodyAsJsonObject().getString("access_token");
                });
    }

    private Uni<String> findUserId(String email, String adminToken) {
        String url = adminBase() + "/users?exact=true&email="
                + URLEncoder.encode(email, StandardCharsets.UTF_8);

        return webClient.getAbs(url)
                .putHeader("Authorization", "Bearer " + adminToken)
                .timeout(10000)
                .send()
                .map(resp -> {
                    if (resp.statusCode() != 200) {
                        throw new KeycloakAdminException("User search failed: HTTP " + resp.statusCode());
                    }
                    JsonArray users = resp.bodyAsJsonArray();
                    return users == null || users.isEmpty() ? null : users.getJsonObject(0).getString("id");
                });
    }

    private Uni<String> createUser(String email, String adminToken) {
        JsonObject newUser = new JsonObject()
                .put("username", email)
                .put("email", email)
                .put("emailVerified", false)
                .put("enabled", true);

        return webClient.postAbs(adminBase() + "/users")
                .putHeader("Authorization", "Bearer " + adminToken)
                .putHeader("Content-Type", "application/json")
                .timeout(10000)
                .sendJsonObject(newUser)
                .map(resp -> {
                    if (resp.statusCode() != 201) {
                        throw new KeycloakAdminException("User creation failed: HTTP " + resp.statusCode());
                    }
                    String location = resp.getHeader("Location");
                    String userId = location.substring(location.lastIndexOf('/') + 1);
                    LOGGER.infof("Created Keycloak user %s for %s", userId, email);
                    return userId;
                });
    }

    private String realmBase() {
        return config.getKeycloakUrl().orElseThrow() + "/realms/" + config.getKeycloakRealm();
    }

    private String adminBase() {
        return config.getKeycloakUrl().orElseThrow() + "/admin/realms/" + config.getKeycloakRealm();
    }

    public static class KeycloakAdminException extends RuntimeException {
        public KeycloakAdminException(String message) {
            super(message);
        }
    }
}
