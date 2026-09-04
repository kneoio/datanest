package com.semantyca.datanest.dto.sharing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.datanest.dto.UploadFileDTO;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.EnumMap;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReceivedSharePublicDTO {
    private String title;
    private String artist;
    private PlaylistItemType type;
    private List<String> genres;
    private List<String> labels;
    private String album;
    private String sharerUserName;
    private String sharerUserEmail;
    private String targetBrandSlug;
    private EnumMap<LanguageCode, String> targetBrandName;
    private int boost;
    private Integer status;
    private boolean notifyOnPlay;
    private String slugName;
    private List<UploadFileDTO> uploadedFiles;
}
