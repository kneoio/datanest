package com.semantyca.datanest.rest;

import io.restassured.RestAssured;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Shared setup for HTTP integration tests against a running server (see {@code live-server-test.properties}).
 */
abstract class AbstractLiveServerIT {

    protected static final String PROPERTIES_FILE = "live-server-test.properties";
    protected static final String BASE_URL_KEY = "datanest.test.base-url";
    protected static final String AUTH_HEADER_KEY = "datanest.test.auth-header";

    protected Path tempAudioFile;

    @AfterEach
    void cleanupTempAudioFile() throws IOException {
        if (tempAudioFile != null) {
            Files.deleteIfExists(tempAudioFile);
        }
    }

    protected static Properties loadTestProperties() throws IOException {
        try (InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (inputStream == null) {
                throw new IOException("Missing test properties file: " + PROPERTIES_FILE);
            }
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        }
    }

    /**
     * Accepts raw JWT or full {@code Authorization} value. Prepends {@code Bearer } when missing.
     */
    protected static String authorizationHeader(String raw) {
        if (raw == null) {
            return "";
        }
        String s = raw.trim();
        if (s.isEmpty()) {
            return "";
        }
        if ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))) {
            s = s.substring(1, s.length() - 1).trim();
        }
        String lower = s.toLowerCase();
        if (lower.startsWith("bearer ")) {
            return "Bearer " + s.substring(7).trim();
        }
        if (lower.startsWith("bearer")) {
            return "Bearer " + s.substring(6).trim();
        }
        return "Bearer " + s;
    }

    /**
     * Validates required properties, sets {@link RestAssured#baseURI}, returns values for requests.
     */
    protected LiveServerContext configureLiveServerFrom(Properties properties) {
        String baseUrl = properties.getProperty(BASE_URL_KEY, "").trim();
        String auth = authorizationHeader(properties.getProperty(AUTH_HEADER_KEY, ""));

        Assumptions.assumeTrue(baseUrl != null && !baseUrl.isBlank(),
                () -> "Set " + BASE_URL_KEY + " in " + PROPERTIES_FILE + " to run live-server upload test");
        Assumptions.assumeTrue(auth != null && !auth.isBlank(),
                () -> "Set " + AUTH_HEADER_KEY + " (raw JWT or 'Bearer <token>') in " + PROPERTIES_FILE + " to run live-server upload test");

        RestAssured.baseURI = baseUrl;
        return new LiveServerContext(baseUrl, auth);
    }

    protected record LiveServerContext(String baseUrl, String authorizationHeader) {
    }
}
