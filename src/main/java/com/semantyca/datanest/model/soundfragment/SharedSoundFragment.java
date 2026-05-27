package com.semantyca.datanest.model.soundfragment;

import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Deprecated
public class SharedSoundFragment {
    private UUID id;
    private OffsetDateTime regDate;
    private OffsetDateTime lastModDate;
    private Long sourceUserId;
    private String sourceUserName;
    private String sourceUserEmail;
    private UUID targetBrandId;
    private UUID soundFragmentId;
    private OffsetDateTime expiresAt;
    private Integer playedCount;
    private Integer ratedCount;
    private Integer status;
    private Integer archived;

    // inflated from JOIN with brands table on write queries
    private String brandSlugName;

    // inflated from JOIN queries with sound_fragments table
    private String title;
    private String artist;
    private PlaylistItemType type;
    private String album;
    private List<UUID> genres;
    private List<UUID> labels;

    // inflated from JOIN with brands table
    private EnumMap<LanguageCode, String> targetBrandName;
}
