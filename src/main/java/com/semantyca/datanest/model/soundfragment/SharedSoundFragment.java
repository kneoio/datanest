package com.semantyca.datanest.model.soundfragment;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
public class SharedSoundFragment {
    private UUID id;
    private UUID sourceBrandId;
    private UUID soundFragmentId;
    private LocalDateTime expiresAt;
    private Integer totalPlayedCount;
    private Integer totalRatedCount;
    private Integer status;
    private Integer archived;
}
