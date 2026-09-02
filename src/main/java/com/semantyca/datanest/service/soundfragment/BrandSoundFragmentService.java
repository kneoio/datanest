package com.semantyca.datanest.service.soundfragment;

import com.semantyca.core.model.user.IUser;
import com.semantyca.datanest.dto.BrandSoundFragmentFlatDTO;
import com.semantyca.datanest.repository.soundfragment.SoundFragmentBrandRepository;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.mixpla.model.filter.SoundFragmentFilter;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class BrandSoundFragmentService {

    private final SoundFragmentBrandRepository repository;
    private final BrandService brandService;

    @Inject
    public BrandSoundFragmentService(SoundFragmentBrandRepository repository, BrandService brandService) {
        this.repository = repository;
        this.brandService = brandService;
    }

    public Uni<List<BrandSoundFragmentFlatDTO>> getBrandSoundFragmentsFlat(String brandName, int limit, int offset,
                                                                           SoundFragmentFilter filter, IUser user) {
        return getBrandSoundFragmentsFlat(brandName, limit, offset, filter, user, false);
    }

    public Uni<List<BrandSoundFragmentFlatDTO>> getBrandSoundFragmentsFlat(String brandName, int limit, int offset,
                                                                           SoundFragmentFilter filter, IUser user,
                                                                           boolean unassignedOnly) {
        if (unassignedOnly || brandName == null || brandName.isBlank()) {
            return repository.findAllFlat(limit, offset, user, filter, unassignedOnly);
        }
        return brandService.getBySlugName(brandName)
                .onItem().transformToUni(brand -> {
                    if (brand == null) {
                        return Uni.createFrom().failure(new IllegalArgumentException("Brand not found: " + brandName));
                    }
                    UUID brandId = brand.getId();
                    return repository.findForBrandFlat(brandId, limit, offset, user, filter)
                            .onItem().transform(fragments -> fragments.isEmpty()
                                    ? Collections.<BrandSoundFragmentFlatDTO>emptyList()
                                    : fragments);
                });
    }

    public Uni<Integer> getBrandSoundFragmentsCount(String brandName, SoundFragmentFilter filter, IUser user) {
        return getBrandSoundFragmentsCount(brandName, filter, user, false);
    }

    public Uni<Integer> getBrandSoundFragmentsCount(String brandName, SoundFragmentFilter filter, IUser user,
                                                    boolean unassignedOnly) {
        if (unassignedOnly || brandName == null || brandName.isBlank()) {
            return repository.findAllCount(user, filter, unassignedOnly);
        }
        return brandService.getBySlugName(brandName)
                .onItem().transformToUni(brand -> {
                    if (brand == null) {
                        return Uni.createFrom().failure(new IllegalArgumentException("Brand not found: " + brandName));
                    }
                    return repository.findForBrandCount(brand.getId(), user, filter);
                });
    }

}

