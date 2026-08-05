package com.semantyca.datanest.rest;

import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.external.KeycloakAdminClient;
import com.semantyca.datanest.service.OtpService;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Optional;

/**
 * Passwordless login, one half of a flow that Keycloak completes.
 *
 * The FE asks datanest for a code, then presents email + code to Keycloak's token endpoint, where a
 * custom direct-grant authenticator calls {@code /internal/verify} here before issuing tokens.
 * Keycloak stays the only token issuer; datanest stays the only place OTP codes live.
 *
 * There is deliberately no token endpoint here - the FE obtains and refreshes tokens directly
 * against Keycloak, so no client secret is ever needed by the browser.
 */
@ApplicationScoped
public class OtpAuthController {
    private static final Logger LOGGER = Logger.getLogger(OtpAuthController.class);
    private static final String INTERNAL_SECRET_HEADER = "X-Internal-Secret";

    private final OtpService otpService;
    private final KeycloakAdminClient keycloakAdminClient;
    private final DatanestConfig config;

    @Inject
    public OtpAuthController(OtpService otpService, KeycloakAdminClient keycloakAdminClient, DatanestConfig config) {
        this.otpService = otpService;
        this.keycloakAdminClient = keycloakAdminClient;
        this.config = config;
    }

    public void setupRoutes(Router router) {
        String base = "/datanest/auth/otp";
        router.route(base + "/*").handler(BodyHandler.create());
        router.post(base + "/request").handler(this::requestCode);
        router.post(base + "/internal/verify").handler(this::internalVerify);
    }

    /**
     * The account is provisioned before the code goes out, so that by the time the user types it back
     * the Keycloak authenticator has a user to resolve.
     */
    private void requestCode(RoutingContext rc) {
        JsonObject body = readBody(rc);
        if (body == null) {
            return;
        }
        String email = body.getString("email");
        if (email == null || !email.contains("@")) {
            fail(rc, 400, "Valid email is required");
            return;
        }
        if (!keycloakAdminClient.isConfigured()) {
            LOGGER.error("OTP requested but Keycloak admin client is not configured");
            fail(rc, 503, "Login is not available");
            return;
        }
        String trimmed = email.trim();

        keycloakAdminClient.findOrCreateUser(trimmed)
                .chain(userId -> otpService.sendOtp(trimmed))
                .subscribe().with(
                        v -> ok(rc, new JsonObject().put("message", "Confirmation code sent")),
                        err -> {
                            LOGGER.errorf("OTP request failed for %s: %s", trimmed, err.getMessage());
                            fail(rc, 500, "Could not send the code");
                        }
                );
    }

    /**
     * Called by the Keycloak authenticator only - never by a browser. The code is consumed here, so a
     * successful call is what buys the tokens Keycloak then issues.
     */
    private void internalVerify(RoutingContext rc) {
        Optional<String> expectedSecret = config.getOtpInternalSecret();
        if (expectedSecret.isEmpty()) {
            LOGGER.error("Internal OTP verify called but datanest.otp.internal-secret is not configured");
            fail(rc, 503, "Not available");
            return;
        }
        if (!secretMatches(expectedSecret.get(), rc.request().getHeader(INTERNAL_SECRET_HEADER))) {
            LOGGER.warn("Internal OTP verify rejected: bad or missing shared secret");
            fail(rc, 403, "Forbidden");
            return;
        }

        JsonObject body = readBody(rc);
        if (body == null) {
            return;
        }
        String email = body.getString("email");
        String code = body.getString("code");
        if (email == null || code == null || email.isBlank() || code.isBlank()) {
            fail(rc, 400, "email and code are required");
            return;
        }
        if (!otpService.verifyAndConsume(email.trim(), code.trim())) {
            fail(rc, 401, "Invalid or expired confirmation code");
            return;
        }
        ok(rc, new JsonObject().put("email", email.trim().toLowerCase()));
    }

    private boolean secretMatches(String expected, String provided) {
        if (provided == null) {
            return false;
        }
        return MessageDigest.isEqual(
                expected.getBytes(StandardCharsets.UTF_8),
                provided.getBytes(StandardCharsets.UTF_8));
    }

    private JsonObject readBody(RoutingContext rc) {
        try {
            JsonObject body = rc.body().asJsonObject();
            if (body == null) {
                fail(rc, 400, "Request body must be a valid JSON object");
                return null;
            }
            return body;
        } catch (Exception e) {
            fail(rc, 400, "Invalid JSON");
            return null;
        }
    }

    private void ok(RoutingContext rc, JsonObject payload) {
        rc.response().setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(payload.encode());
    }

    private void fail(RoutingContext rc, int status, String message) {
        rc.response().setStatusCode(status)
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject().put("error", message).encode());
    }
}
