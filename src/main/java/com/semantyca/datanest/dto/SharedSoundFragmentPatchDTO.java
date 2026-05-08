package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Add or remove destination brands ({@code mixpla__shared_sound_fragments.source_brand_id})
 * for a sound fragment the user may edit.
 */
@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SharedSoundFragmentPatchDTO {
    private List<UUID> addBrandIds = new ArrayList<>();
    private List<UUID> removeBrandIds = new ArrayList<>();
}
