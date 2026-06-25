package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.dto.scheduler.ScheduleDTO;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.mixpla.model.PlayHistory;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SoundFragmentDTO extends SoundFragmentFlatDTO {
    private List<String> brands;
    private List<String> newlyUploaded;
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

    public SoundFragmentDTO(String id) {
        this.id = UUID.fromString(id);
    }
}
