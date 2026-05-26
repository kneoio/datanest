package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StagePlaylistDTO {

    @JsonDeserialize(using = StringOrArrayDeserializer.class)
    private String sourcing;

    static class StringOrArrayDeserializer extends JsonDeserializer<String> {
        @Override
        public String deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            if (p.isExpectedStartArrayToken()) {
                String first = null;
                while (p.nextToken() != com.fasterxml.jackson.core.JsonToken.END_ARRAY) {
                    if (first == null) first = p.getText();
                }
                return first;
            }
            return p.getText();
        }
    }
    private String title;
    private String artist;
    private List<UUID> genres;
    private List<UUID> labels;
    private List<String> type;
    private List<String> source;
    private String searchTerm;
    private List<UUID> soundFragments;
    private List<ScenePromptDTO> prompts;
}
