package com.semantyca.datanest.rest;

import io.restassured.response.Response;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

class SoundFragmentUploadLiveServerIT extends AbstractLiveServerIT {

    private static final String GENRE_ID_KEY = "datanest.test.genre-id";
    private static final String BRAND_ID_KEY = "datanest.test.brand-id";

    @Test
    void shouldUploadSoundFragmentFileAgainstRunningServer() throws IOException {
        Properties properties = loadTestProperties();
        LiveServerContext ctx = configureLiveServerFrom(properties);

        tempAudioFile = Files.createTempFile("upload-live-", ".mp3");
        Files.write(tempAudioFile, new byte[]{0x49, 0x44, 0x33, 0x04, 0x00, 0x00});

        String uploadId = UUID.randomUUID().toString();
        Response response = given()
                .header("Authorization", ctx.authorizationHeader())
                .multiPart("file", tempAudioFile.toFile(), "audio/mpeg")
                .queryParam("uploadId", uploadId)
                .when()
                .post("/datanest/soundfragments/files/temp")
                .then()
                .statusCode(200)
                .body("status", equalTo("finished"))
                .body("batchId", equalTo(uploadId))
                .body("name", notNullValue())
                .body("url", notNullValue())
                .extract()
                .response();

        String uploadedName = response.jsonPath().getString("name");
        String uploadedUrl = response.jsonPath().getString("url");

        org.junit.jupiter.api.Assertions.assertTrue(uploadedName.endsWith(".mp3"),
                "Uploaded file name should keep .mp3 extension");
        org.junit.jupiter.api.Assertions.assertTrue(uploadedUrl.contains("/datanest/soundfragments/files/temp/"),
                "Uploaded URL should point to temp file endpoint");
    }

    @Test
    void shouldUseUploadedFileWhenUpsertingNewSoundFragment() throws IOException {
        Properties properties = loadTestProperties();
        LiveServerContext ctx = configureLiveServerFrom(properties);
        String genreId = properties.getProperty(GENRE_ID_KEY, "").trim();
        String brandId = properties.getProperty(BRAND_ID_KEY, "").trim();

        Assumptions.assumeTrue(!genreId.isBlank(),
                () -> "Set " + GENRE_ID_KEY + " in " + PROPERTIES_FILE + " to run upsert test");
        Assumptions.assumeTrue(!brandId.isBlank(),
                () -> "Set " + BRAND_ID_KEY + " in " + PROPERTIES_FILE + " to run upsert test");

        tempAudioFile = Files.createTempFile("upload-upsert-live-", ".mp3");
        Files.write(tempAudioFile, new byte[]{0x49, 0x44, 0x33, 0x04, 0x00, 0x00});

        String uploadId = UUID.randomUUID().toString();
        String uploadedName = given()
                .header("Authorization", ctx.authorizationHeader())
                .multiPart("file", tempAudioFile.toFile(), "audio/mpeg")
                .queryParam("uploadId", uploadId)
                .when()
                .post("/datanest/soundfragments/files/temp")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath()
                .getString("name");

        String title = "it-upload-upsert-" + UUID.randomUUID();
        String artist = "it-artist-" + UUID.randomUUID();
        Map<String, Object> payload = new HashMap<>();
        payload.put("type", "SONG");
        payload.put("title", title);
        payload.put("artist", artist);
        payload.put("genres", List.of(genreId));
        payload.put("representedInBrands", List.of(brandId));
        payload.put("newlyUploaded", List.of(uploadedName));

        String responseBody = given()
                .header("Authorization", ctx.authorizationHeader())
                .contentType("application/json")
                .body(payload)
                .when()
                .post("/datanest/soundfragments/new")
                .then()
                .statusCode(anyOf(equalTo(200), equalTo(201)))
                .body(containsString(uploadedName))
                .extract()
                .asString();

        org.junit.jupiter.api.Assertions.assertTrue(responseBody.contains(uploadedName),
                "Upsert response should contain uploaded file name from newlyUploaded");
    }
}
