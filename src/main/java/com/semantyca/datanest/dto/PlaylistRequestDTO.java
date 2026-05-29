package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
import com.semantyca.mixpla.model.cnst.MergingTypeMeta;
import com.semantyca.mixpla.model.cnst.MixingType;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PlaylistRequestDTO {
    private MixingType mixingType;
    private Map<String, String> mixingArtefacts;
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

    public List<String> getAvailableMixingTypes() {
        if (!"GENERATED".equals(sourcing)) return null;
        return Arrays.stream(MixingType.values())
                .filter(t -> MergingTypeMeta.of(t).hasGeneratedContent())
                .map(Enum::name)
                .toList();
    }

    public List<String> getArtifactSlots() {
        if (mixingType == null || !MergingTypeMeta.of(mixingType).hasGeneratedContent()) return null;
        return MergingTypeMeta.of(mixingType).requiredSongKeys().stream()
                .map(Enum::name)
                .sorted()
                .toList();
    }
}
