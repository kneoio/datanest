package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.DurationSerializer;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.dto.scheduler.ScheduleDTO;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.mixpla.model.PlayHistory;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SourceType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SoundFragmentPublicDTO {
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
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    private Map<String, Object> addInfo;
    private List<String> brands;
    private List<UploadFileDTO> uploadedFiles;
    private ScheduleDTO schedule;
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<PlayHistory> playHistory;
    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<ShareDTO> sharedWith;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
}
