package com.semantyca.datanest.dto.sharing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SharedSoundDTO {
    private UUID id;
    private String title;
    private String artist;
    private PlaylistItemType type;
    private String album;
    private List<UUID> genres;
    private List<UUID> labels;
    private List<SharedSoundFragmentDTO> shares;  // ?????
}
