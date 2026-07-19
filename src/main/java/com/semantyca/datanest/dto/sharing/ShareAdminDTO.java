package com.semantyca.datanest.dto.sharing;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ShareAdminDTO {
    private UUID id;
    private Long sourceUserId;
    private String sourceUserName;
    private String sourceUserEmail;
    @NotNull
    private UUID targetBrandId;
    @NotNull
    private UUID soundFragmentId;
    private OffsetDateTime expiresAt;
    private Integer boost;
    private Integer status;
    private boolean notifyOnPlay;
}
