package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.DurationSerializer;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SourceType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SoundFragmentPublicFlatDTO {
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd.MM.yyyy HH:mm")
    private ZonedDateTime regDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd.MM.yyyy HH:mm")
    private ZonedDateTime lastModifiedDate;
    private SourceType source;
    private String streamUrl;
    private Integer status = -1;
    private PlaylistItemType type;
    private String title;
    private String artist;
    private UUID artistId;
    private List<UUID> genres;
    private List<UUID> labels;
    private String album;
    private String slugName;
    @JsonSerialize(using = DurationSerializer.class)
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private Duration length;
    private int boost;
    private String description;
    private List<UUID> representedInBrands;
    private OffsetDateTime expiresAt;
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private boolean shared;
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private boolean scheduled;
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private int likes;
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private int dislikes;
}
