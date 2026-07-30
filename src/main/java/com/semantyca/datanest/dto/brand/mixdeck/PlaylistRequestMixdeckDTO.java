package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.mixpla.model.cnst.MixingType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PlaylistRequestMixdeckDTO {
    private MixingType mixingType;
    private Map<String, String> mixingArtefacts;
    private String sourcing;
    private String title;
    private String artist;
    private List<String> genres;
    private List<String> labels;
    private List<String> type;
    private List<String> source;
    private String searchTerm;
    private List<String> soundFragments;
    private List<ScenePromptMixdeckDTO> prompts;
}
