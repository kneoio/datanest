package com.semantyca.datanest.service;

import com.semantyca.datanest.repository.BrandRepository;
import com.semantyca.mixpla.model.MixplaUserSubscription;
import com.semantyca.mixpla.repository.UserSubscriptionRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class UserSubscriptionService {

    @Inject
    BrandRepository brandRepository;

    @Inject
    UserSubscriptionRepository userSubscriptionRepository;

    public Uni<MixplaUserSubscription> getActiveSubscriptionForBrand(String slug) {
        return brandRepository.getBySlugName(slug)
                .onItem().transformToUni(brand -> {
                    if (brand.getOwner() == null || brand.getOwner().getUserId() == null) {
                        return Uni.createFrom().nullItem();
                    }
                    return userSubscriptionRepository.findActiveByUserId(brand.getOwner().getUserId());
                });
    }
}