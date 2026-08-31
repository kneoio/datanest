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
        return Uni.combine().all().unis(
                        getActiveSubscription(user),
                        brandRepository.getAllCount(user, false, null))
                .asTuple()
                .onItem().transformToUni(tuple -> {
                    MixplaUserSubscription subscription = tuple.getItem1();
                    Integer stationCount = tuple.getItem2();
                    if (!canCreate(subscription, stationCount)) {
                        String type = subscription != null ? subscription.getSubscriptionType() : null;
                        Integer max = subscription != null ? subscription.getMaxStations() : null;
                        String message = max == null
                                ? "Station limit reached: no active subscription found"
                                : "Station limit reached: your subscription allows " + max + " stations";
                        return Uni.createFrom().failure(new StationLimitException(message, type, max, stationCount));
                    }
                    return Uni.createFrom().voidItem();
                });
    }

    public Uni<Boolean> canCreateStation(IUser user) {
        return Uni.combine().all().unis(
                        getActiveSubscription(user),
                        brandRepository.getAllCount(user, false, null))
                .asTuple()
                .map(tuple -> canCreate(tuple.getItem1(), tuple.getItem2()));
    }

    public static boolean canCreate(MixplaUserSubscription subscription, Integer stationCount) {
        Integer max = subscription != null ? subscription.getMaxStations() : null;
        return max != null && max > 0 && stationCount != null && stationCount < max;
    }

    public static class StationLimitException extends IllegalStateException {
        private final String subscriptionType;
        private final Integer maxStations;
        private final Integer stationCount;

        public StationLimitException(String message, String subscriptionType,
                                     Integer maxStations, Integer stationCount) {
            super(message);
            this.subscriptionType = subscriptionType;
            this.maxStations = maxStations;
            this.stationCount = stationCount;
        }

        public String getSubscriptionType() {
            return subscriptionType;
        }

        public Integer getMaxStations() {
            return maxStations;
        }

        public Integer getStationCount() {
            return stationCount;
        }
    }
}