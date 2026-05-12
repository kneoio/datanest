package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.DurationSerializer;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SharedSoundFragmentPreviewDTO {
    private UUID id;
    private String title;
    private String artist;
    private PlaylistItemType type;
    private List<UUID> genres;
    private List<UUID> labels;
    private String album;
}
