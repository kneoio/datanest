package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SharedSoundFragmentDTO {
    private UUID id;
    private UUID sourceBrandId;
    private UUID soundFragmentId;
    private LocalDateTime expiresAt;
    private Integer totalPlayedCount;
    private Integer totalRatedCount;
    private Integer status;
    private Integer archived;
}
