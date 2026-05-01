package com.semantyca.datanest.rest;

import io.restassured.specification.RequestSpecification;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

/**
 * Live-server tests for {@link SoundFragmentBulkUploadController} (no mocks).
 */
class SoundFragmentBulkUploadLiveServerIT extends AbstractLiveServerIT {

    private static final String BRAND_SLUG_KEY = "datanest.test.brand-slug";

    /**
     * {@code POST /datanest/soundfragments-bulk/files} then SSE progress until terminal state.
     * Uses a tiny valid WAV so ffprobe can populate metadata (required for entity creation).
     */
    @Test
    void shouldBulkUploadSoundFragmentEndToEnd() throws IOException {
        Properties properties = loadTestProperties();
        LiveServerContext ctx = configureLiveServerFrom(properties);
        String brandSlug = properties.getProperty(BRAND_SLUG_KEY, "").trim();

        String batchId = UUID.randomUUID().toString();
        String fileId = UUID.randomUUID().toString();

        tempAudioFile = Files.createTempFile("bulk-live-", ".wav");
        Files.write(tempAudioFile, tinyWavBytes());

        RequestSpecification post = given()
                .header("Authorization", ctx.authorizationHeader())
                .multiPart("file", tempAudioFile.toFile(), "audio/wav")
                .queryParam("batchId", batchId)
                .queryParam("fileId", fileId);
        if (!brandSlug.isBlank()) {
            post = post.queryParam("brandSlug", brandSlug);
        }

        post.when()
                .post("/datanest/soundfragments-bulk/files")
                .then()
                .statusCode(200)
                .body("batchId", equalTo(batchId))
                .body("id", equalTo(fileId))
                .body("status", equalTo("processing"));

        String sseBody = given()
                .header("Authorization", ctx.authorizationHeader())
                .config(io.restassured.config.RestAssuredConfig.config()
                        .httpClient(io.restassured.config.HttpClientConfig.httpClientConfig()
                                .setParam("http.socket.timeout", 120000)
                                .setParam("http.connection.timeout", 15000)))
                .when()
                .get("/datanest/soundfragments-bulk/status/" + batchId + "/stream")
                .then()
                .statusCode(200)
                .extract()
                .asString();

        org.junit.jupiter.api.Assertions.assertTrue(sseBody.contains(fileId),
                "SSE stream should include fileId " + fileId);
        if (sseBody.contains("\"status\":\"error\"")) {
            org.junit.jupiter.api.Assertions.fail("Bulk pipeline reported error. SSE tail: "
                    + sseBody.substring(Math.max(0, sseBody.length() - 2500)));
        }
        org.junit.jupiter.api.Assertions.assertTrue(sseBody.contains("\"status\":\"finished\""),
                "Expected finished status in SSE stream");
    }

    private static byte[] tinyWavBytes() throws IOException {
        int sampleRate = 8000;
        int numSamples = 800;
        short numChannels = 1;
        short bitsPerSample = 16;
        int byteRate = sampleRate * numChannels * bitsPerSample / 8;
        short blockAlign = (short) (numChannels * bitsPerSample / 8);
        int dataSize = numSamples * blockAlign;
        int chunkSize = 36 + dataSize;

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes("RIFF".getBytes(StandardCharsets.US_ASCII));
        writeIntLe(out, chunkSize);
        out.writeBytes("WAVE".getBytes(StandardCharsets.US_ASCII));
        out.writeBytes("fmt ".getBytes(StandardCharsets.US_ASCII));
        writeIntLe(out, 16);
        writeShortLe(out, (short) 1);
        writeShortLe(out, numChannels);
        writeIntLe(out, sampleRate);
        writeIntLe(out, byteRate);
        writeShortLe(out, blockAlign);
        writeShortLe(out, bitsPerSample);
        out.writeBytes("data".getBytes(StandardCharsets.US_ASCII));
        writeIntLe(out, dataSize);
        for (int i = 0; i < dataSize; i++) {
            out.write(0);
        }
        return out.toByteArray();
    }

    private static void writeIntLe(ByteArrayOutputStream out, int v) throws IOException {
        out.write(v & 0xff);
        out.write((v >> 8) & 0xff);
        out.write((v >> 16) & 0xff);
        out.write((v >> 24) & 0xff);
    }

    private static void writeShortLe(ByteArrayOutputStream out, short v) throws IOException {
        out.write(v & 0xff);
        out.write((v >> 8) & 0xff);
    }
}
