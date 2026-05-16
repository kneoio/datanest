package com.semantyca.datanest.dto.sharing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ShareDTO extends AbstractDTO {
    private UUID id;
    private long sourceUserId;
    private String sourceUserName;
    private String sourceUserEmail;
    private UUID targetBrandId;
    private UUID soundFragmentId;
    private LocalDateTime expiresAt;
    private int playedCount;
    private int ratedCount;
    private int status;
    private int archived;
}
