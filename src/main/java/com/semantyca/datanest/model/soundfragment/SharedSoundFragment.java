package com.semantyca.datanest.model.soundfragment;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
public class SharedSoundFragment {
    private UUID id;
    private Long sourceUserId;
    private String sourceUserName;
    private String sourceUserEmail;
    private UUID targetBrandId;
    private UUID soundFragmentId;
    private LocalDateTime expiresAt;
    private Integer playedCount;
    private Integer ratedCount;
    private Integer status;
    private Integer archived;
}
