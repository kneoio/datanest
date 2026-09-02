package com.semantyca.datanest.service;

import com.semantyca.core.model.user.IUser;
import com.semantyca.datanest.repository.BrandRepository;
import com.semantyca.datanest.repository.soundfragment.SoundFragmentRepository;
import com.semantyca.mixpla.model.MixplaUserSubscription;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.filter.SoundFragmentFilter;
import com.semantyca.mixpla.repository.UserSubscriptionRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

@ApplicationScoped
public class UserSubscriptionService {

    @Inject
    BrandRepository brandRepository;

    @Inject
    SoundFragmentRepository soundFragmentRepository;

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
                    if (!canCreateStation(subscription, stationCount)) {
                        String type = subscription != null ? subscription.getSubscriptionType() : null;
                        Integer max = subscription != null ? subscription.getMaxStations() : null;
                        String message = max == null
                                ? "Station limit reached: no active subscription found"
                                : "Station limit reached: your subscription allows " + max + " stations";
                        return Uni.createFrom().failure(EntitlementLimitException.station(message, type, max, stationCount));
                    }
                    return Uni.createFrom().voidItem();
                });
    }

    public Uni<Boolean> canCreateStation(IUser user) {
        return Uni.combine().all().unis(
                        getActiveSubscription(user),
                        brandRepository.getAllCount(user, false, null))
                .asTuple()
                .map(tuple -> canCreateStation(tuple.getItem1(), tuple.getItem2()));
    }

    public static boolean canCreateStation(MixplaUserSubscription subscription, Integer stationCount) {
        return withinLimit(subscription != null ? subscription.getMaxStations() : null, stationCount);
    }

    public Uni<Void> assertCanCreateSong(IUser user) {
        return Uni.combine().all().unis(
                        getActiveSubscription(user),
                        songCount(user))
                .asTuple()
                .onItem().transformToUni(tuple -> {
                    MixplaUserSubscription subscription = tuple.getItem1();
                    Integer songCount = tuple.getItem2();
                    if (!canCreateSong(subscription, songCount)) {
                        String type = subscription != null ? subscription.getSubscriptionType() : null;
                        Integer max = subscription != null ? subscription.getMaxSongs() : null;
                        String message = max == null
                                ? "Song limit reached: no active subscription found"
                                : "Song limit reached: your subscription allows " + max + " songs";
                        return Uni.createFrom().failure(EntitlementLimitException.song(message, type, max, songCount));
                    }
                    return Uni.createFrom().voidItem();
                });
    }

    public Uni<Boolean> canCreateSong(IUser user) {
        return Uni.combine().all().unis(
                        getActiveSubscription(user),
                        songCount(user))
                .asTuple()
                .map(tuple -> canCreateSong(tuple.getItem1(), tuple.getItem2()));
    }

    public static boolean canCreateSong(MixplaUserSubscription subscription, Integer songCount) {
        return withinLimit(subscription != null ? subscription.getMaxSongs() : null, songCount);
    }

    private static boolean withinLimit(Integer max, Integer count) {
        return max != null && max > 0 && count != null && count < max;
    }

    private Uni<Integer> songCount(IUser user) {
        SoundFragmentFilter filter = new SoundFragmentFilter();
        filter.setSource(List.of(SourceType.USER_UPLOAD));
        filter.setAuthor(user.getId().intValue());
        filter.setActivated(true);
        return soundFragmentRepository.getAllCount(user, false, filter);
    }

    public static class EntitlementLimitException extends IllegalStateException {
        private final String code;
        private final String title;
        private final String upgradeHint;
        private final String subscriptionType;
        private final String maxField;
        private final Integer maxValue;
        private final String countField;
        private final Integer countValue;

        private EntitlementLimitException(String message, String code, String title, String upgradeHint,
                                          String subscriptionType, String maxField, Integer maxValue,
                                          String countField, Integer countValue) {
            super(message);
            this.code = code;
            this.title = title;
            this.upgradeHint = upgradeHint;
            this.subscriptionType = subscriptionType;
            this.maxField = maxField;
            this.maxValue = maxValue;
            this.countField = countField;
            this.countValue = countValue;
        }

        public static EntitlementLimitException station(String message, String subscriptionType,
                                                        Integer maxStations, Integer stationCount) {
            return new EntitlementLimitException(message, "STATION_LIMIT_REACHED", "Station limit reached",
                    "With a Plus subscription you can create a brand.",
                    subscriptionType, "maxStations", maxStations, "stationCount", stationCount);
        }

        public static EntitlementLimitException song(String message, String subscriptionType,
                                                     Integer maxSongs, Integer songCount) {
            return new EntitlementLimitException(message, "SONG_LIMIT_REACHED", "Song limit reached",
                    "With a Plus subscription you can add more songs.",
                    subscriptionType, "maxSongs", maxSongs, "songCount", songCount);
        }

        public String getCode() {
            return code;
        }

        public String getTitle() {
            return title;
        }

        public String getUpgradeHint() {
            return upgradeHint;
        }

        public String getSubscriptionType() {
            return subscriptionType;
        }

        public String getMaxField() {
            return maxField;
        }

        public Integer getMaxValue() {
            return maxValue;
        }

        public String getCountField() {
            return countField;
        }

        public Integer getCountValue() {
            return countValue;
        }
    }
}