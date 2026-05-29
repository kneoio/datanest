package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
import com.semantyca.mixpla.model.cnst.MergingTypeMeta;
import com.semantyca.mixpla.model.cnst.MixingType;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PlaylistRequestDTO {
    private MixingType mixingType;
    private Map<String, String> mixingArtefacts;

    private static final java.util.Set<MixingType> ARTIFACT_SLOT_TYPES = java.util.Set.of(
            MixingType.JINGLE_GENERATED_JINGLE,
            MixingType.JINGLE_GENERATED_JINGLE_WITH_BACKGROUND
    );

    public List<String> getArtifactSlots() {
        if (mixingType == null || !ARTIFACT_SLOT_TYPES.contains(mixingType)) return null;
        return MergingTypeMeta.of(mixingType).requiredSongKeys().stream()
                .map(Enum::name)
                .sorted()
                .toList();
    }
    private String sourcing;
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
