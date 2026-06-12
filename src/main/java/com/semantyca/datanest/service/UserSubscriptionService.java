package com.semantyca.datanest.service;

import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.subscription.UserSubscriptionDTO;
import com.semantyca.datanest.repository.UserSubscriptionRepository;
import com.semantyca.mixpla.model.MixplaUserSubscription;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class UserSubscriptionService {

    private final UserSubscriptionRepository repository;
    private final UserService userService;

    protected UserSubscriptionService() {
        this.repository = null;
        this.userService = null;
    }

    @Inject
    public UserSubscriptionService(UserSubscriptionRepository repository, UserService userService) {
        this.repository = repository;
        this.userService = userService;
    }

    public Uni<Integer> getAllCount() {
        assert repository != null;
        return repository.getAllCount();
    }

    public Uni<List<UserSubscriptionDTO>> getAll(int limit, int offset) {
        assert repository != null;
        return repository.getAll(limit, offset)
                .chain(list -> {
                    var unis = list.stream().map(this::mapToDTO).collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<UserSubscriptionDTO> findById(UUID id) {
        assert repository != null;
        return repository.findById(id).chain(this::mapToDTO);
    }

    public Uni<UserSubscriptionDTO> upsert(String id, UserSubscriptionDTO dto, IUser user) {
        assert repository != null;
        MixplaUserSubscription doc = new MixplaUserSubscription();
        doc.setUserId(dto.getUserId());
        doc.setStripeCustomerId(dto.getStripeCustomerId());
        doc.setStripeSubscriptionId(dto.getStripeSubscriptionId());
        doc.setSubscriptionType(dto.getSubscriptionType());
        doc.setSubscriptionStatus(dto.getSubscriptionStatus());
        doc.setTrialEnd(dto.getTrialEnd());
        doc.setCurrentPeriodStart(dto.getCurrentPeriodStart());
        doc.setCurrentPeriodEnd(dto.getCurrentPeriodEnd());
        doc.setCancelAt(dto.getCancelAt());
        doc.setCanceledAt(dto.getCanceledAt());
        doc.setActive(dto.isActive());
        doc.setStreamDurationMinutes(dto.getStreamDurationMinutes());
        doc.setOtsAllowed(dto.isOtsAllowed());
        doc.setMaxSongs(dto.getMaxSongs());
        doc.setStreamQualityKbps(dto.getStreamQualityKbps());
        doc.setSupportLevel(dto.getSupportLevel());
        doc.setCustomScriptAllowed(dto.isCustomScriptAllowed());

        boolean isNew = "new".equalsIgnoreCase(id) || id == null;

        return repository.resolveProductId(dto.getDjTypeId())
                .invoke(doc::setDjTypeId)
                .chain(() -> isNew
                        ? repository.insert(doc, user)
                        : repository.update(UUID.fromString(id), doc, user))
                .chain(this::mapToDTO);
    }

    private Uni<UserSubscriptionDTO> mapToDTO(MixplaUserSubscription s) {
        assert repository != null;
        assert userService != null;
        UserSubscriptionDTO dto = UserSubscriptionDTO.from(s);
        Uni<UserSubscriptionDTO> withUser = userService.findById(dto.getUserId())
                .map(opt -> {
                    opt.ifPresent(u -> dto.setUser(new UserSubscriptionDTO.UserRef(u.getLogin(), u.getEmail())));
                    return dto;
                });
        Uni<UserSubscriptionDTO> withIdentifier = s.getDjTypeId() != null
                ? repository.resolveIdentifier(s.getDjTypeId()).map(identifier -> {
                    dto.setDjTypeId(identifier);
                    return dto;
                })
                : Uni.createFrom().item(dto);
        return Uni.combine().all().unis(withUser, withIdentifier).asTuple().map(t -> dto);
    }
}
