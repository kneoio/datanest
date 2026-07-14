package com.semantyca.datanest.dto.sharing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.datanest.dto.UploadFileDTO;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.EnumMap;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SharingPreviewDTO extends AbstractDTO {
    private UUID id;
    private String title;
    private String artist;
    private PlaylistItemType type;
    private List<UUID> genres;
    private List<UUID> labels;
    private String album;
    private String sharerUserName;
    private String sharerUserEmail;
    private EnumMap<LanguageCode, String> targetBrandName;
    private int boost;
    private Integer status;
    private boolean notifyOnPlay;
    // Lets the receiver preview audio before accepting/rejecting - only populated on the
    // single-item fetch (getById), not the paged received list.
    private List<UploadFileDTO> uploadedFiles;
}
