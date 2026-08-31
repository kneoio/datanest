package com.semantyca.datanest.service;

import com.semantyca.core.model.user.IUser;
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

    public Uni<MixplaUserSubscription> getActiveSubscription(IUser user) {
        return userSubscriptionRepository.findActiveByUserId(user.getId());
    }

    public Uni<MixplaUserSubscription> getActiveSubscriptionForBrand(String slug) {
        return brandRepository.getBySlugName(slug)
                .onItem().transformToUni(brand -> {
                    if (brand.getOwner() == null || brand.getOwner().getUserId() == null) {
                        return Uni.createFrom().nullItem();
                    }
                    return userSubscriptionRepository.findActiveByUserId(brand.getOwner().getUserId());
                });
    }

    public Uni<Void> assertCanCreateStation(IUser user) {
        return getActiveSubscription(user)
                .onItem().transformToUni(subscription -> {
                    if (subscription == null || subscription.getMaxStations() == null) {
                        return Uni.createFrom().failure(new IllegalStateException(
                                "Station limit reached: no active subscription found"));
                    }
                    return brandRepository.getAllCount(user, false, null)
                            .chain(count -> count >= subscription.getMaxStations()
                                    ? Uni.createFrom().failure(new IllegalStateException(
                                            "Station limit reached: your subscription allows "
                                                    + subscription.getMaxStations() + " stations"))
                                    : Uni.createFrom().voidItem());
                });
    }
}