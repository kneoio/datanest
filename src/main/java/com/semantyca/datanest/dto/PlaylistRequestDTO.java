package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
import com.semantyca.mixpla.model.cnst.MergingTypeMeta;
import com.semantyca.mixpla.model.cnst.MixingType;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;
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

    private static final List<MixingType> GENERATED_MIXING_TYPES = List.of(
            MixingType.JINGLE_GENERATED_JINGLE_WITH_BACKGROUND,
            MixingType.JINGLE_GENERATED_JINGLE
    );
}
