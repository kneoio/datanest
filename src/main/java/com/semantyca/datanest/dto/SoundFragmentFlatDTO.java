package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.DurationSerializer;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SourceType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SoundFragmentFlatDTO extends AbstractDTO {
    private SourceType source;
    private String streamUrl;
    private Integer status = -1;
    @NotNull
    private PlaylistItemType type;
    @NotBlank
    private String title;
    @NotBlank
    private String artist;
    private UUID artistId;
    @NotNull
    @NotEmpty
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
