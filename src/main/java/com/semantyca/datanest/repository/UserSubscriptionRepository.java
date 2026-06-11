package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.table.TableNameResolver;
import com.semantyca.mixpla.model.UserSubscription;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class UserSubscriptionRepository extends AsyncRepository {

    private static final String TABLE = TableNameResolver.create()
            .getEntityNames(TableNameResolver.USER_SUBSCRIPTION_ENTITY_NAME)
            .getTableName();

    @Inject
    public UserSubscriptionRepository(Pool client, ObjectMapper mapper) {
        super(client, mapper, null);
    }

    public Uni<List<UserSubscription>> getAll(int limit, int offset) {
        String sql = String.format("SELECT * FROM %s ORDER BY reg_date DESC LIMIT %s OFFSET %s", TABLE, limit, offset);
        return client.query(sql)
                .execute()
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> getAllCount() {
        return getAllCount(TABLE);
    }

    public Uni<UserSubscription> findById(UUID id) {
        String sql = String.format("SELECT * FROM %s WHERE id=$1", TABLE);
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(rows -> {
                    if (!rows.iterator().hasNext()) throw new DocumentHasNotFoundException(id);
                    return from(rows.iterator().next());
                });
    }

    public Uni<List<UserSubscription>> findByUserId(Long userId) {
        String sql = String.format("SELECT * FROM %s WHERE user_id=$1 ORDER BY reg_date DESC", TABLE);
        return client.preparedQuery(sql)
                .execute(Tuple.of(userId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<UserSubscription> insert(UserSubscription doc, IUser user) {
        String sql = String.format(
                "INSERT INTO %s (author, reg_date, last_mod_user, last_mod_date, user_id, stripe_customer_id, stripe_subscription_id, subscription_type, subscription_status, trial_end, current_period_start, current_period_end, cancel_at, canceled_at, active, stream_duration_minutes, ots_allowed, max_songs, stream_quality_kbps, dj_type_id, support_level, custom_script_allowed) " +
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22) RETURNING id", TABLE);
        OffsetDateTime now = OffsetDateTime.now();
        Tuple params = Tuple.of(user.getId())
                .addOffsetDateTime(now)
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addLong(doc.getUserId())
                .addString(doc.getStripeCustomerId())
                .addString(doc.getStripeSubscriptionId())
                .addString(doc.getSubscriptionType())
                .addString(doc.getSubscriptionStatus())
                .addValue(doc.getTrialEnd() != null ? doc.getTrialEnd().toOffsetDateTime() : null)
                .addValue(doc.getCurrentPeriodStart() != null ? doc.getCurrentPeriodStart().toOffsetDateTime() : null)
                .addValue(doc.getCurrentPeriodEnd() != null ? doc.getCurrentPeriodEnd().toOffsetDateTime() : null)
                .addValue(doc.getCancelAt() != null ? doc.getCancelAt().toOffsetDateTime() : null)
                .addValue(doc.getCanceledAt() != null ? doc.getCanceledAt().toOffsetDateTime() : null)
                .addBoolean(doc.isActive())
                .addValue(doc.getStreamDurationMinutes())
                .addBoolean(doc.isOtsAllowed())
                .addValue(doc.getMaxSongs())
                .addValue(doc.getStreamQualityKbps())
                .addValue(doc.getDjTypeId())
                .addShort(doc.getSupportLevel())
                .addBoolean(doc.isCustomScriptAllowed());
        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transformToUni(result -> findById(result.iterator().next().getUUID("id")));
    }

    public Uni<UserSubscription> update(UUID id, UserSubscription doc, IUser user) {
        String sql = String.format(
                "UPDATE %s SET last_mod_user=$1, last_mod_date=$2, stripe_customer_id=$3, subscription_type=$4, subscription_status=$5, trial_end=$6, current_period_start=$7, current_period_end=$8, cancel_at=$9, canceled_at=$10, active=$11, stream_duration_minutes=$12, ots_allowed=$13, max_songs=$14, stream_quality_kbps=$15, dj_type_id=$16, support_level=$17, custom_script_allowed=$18 WHERE id=$19", TABLE);
        Tuple params = Tuple.of(user.getId())
                .addOffsetDateTime(OffsetDateTime.now())
                .addString(doc.getStripeCustomerId())
                .addString(doc.getSubscriptionType())
                .addString(doc.getSubscriptionStatus())
                .addValue(doc.getTrialEnd() != null ? doc.getTrialEnd().toOffsetDateTime() : null)
                .addValue(doc.getCurrentPeriodStart() != null ? doc.getCurrentPeriodStart().toOffsetDateTime() : null)
                .addValue(doc.getCurrentPeriodEnd() != null ? doc.getCurrentPeriodEnd().toOffsetDateTime() : null)
                .addValue(doc.getCancelAt() != null ? doc.getCancelAt().toOffsetDateTime() : null)
                .addValue(doc.getCanceledAt() != null ? doc.getCanceledAt().toOffsetDateTime() : null)
                .addBoolean(doc.isActive())
                .addValue(doc.getStreamDurationMinutes())
                .addBoolean(doc.isOtsAllowed())
                .addValue(doc.getMaxSongs())
                .addValue(doc.getStreamQualityKbps())
                .addValue(doc.getDjTypeId())
                .addShort(doc.getSupportLevel())
                .addBoolean(doc.isCustomScriptAllowed())
                .addUUID(id);
        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transformToUni(rowSet -> {
                    if (rowSet.rowCount() == 0) return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                    return findById(id);
                });
    }

    public Uni<Integer> delete(UUID id) {
        String sql = String.format("DELETE FROM %s WHERE id=$1", TABLE);
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(RowSet::rowCount);
    }

    private UserSubscription from(Row row) {
        UserSubscription s = new UserSubscription();
        s.setId(row.getUUID("id"));
        s.setUserId(row.getLong("user_id"));
        s.setStripeCustomerId(row.getString("stripe_customer_id"));
        s.setStripeSubscriptionId(row.getString("stripe_subscription_id"));
        s.setSubscriptionType(row.getString("subscription_type"));
        s.setSubscriptionStatus(row.getString("subscription_status"));
        var trialEnd = row.getOffsetDateTime("trial_end");
        if (trialEnd != null) s.setTrialEnd(trialEnd.toZonedDateTime());
        var periodStart = row.getOffsetDateTime("current_period_start");
        if (periodStart != null) s.setCurrentPeriodStart(periodStart.toZonedDateTime());
        var periodEnd = row.getOffsetDateTime("current_period_end");
        if (periodEnd != null) s.setCurrentPeriodEnd(periodEnd.toZonedDateTime());
        var cancelAt = row.getOffsetDateTime("cancel_at");
        if (cancelAt != null) s.setCancelAt(cancelAt.toZonedDateTime());
        var canceledAt = row.getOffsetDateTime("canceled_at");
        if (canceledAt != null) s.setCanceledAt(canceledAt.toZonedDateTime());
        s.setActive(Boolean.TRUE.equals(row.getBoolean("active")));
        s.setStreamDurationMinutes(row.getInteger("stream_duration_minutes"));
        s.setOtsAllowed(Boolean.TRUE.equals(row.getBoolean("ots_allowed")));
        s.setMaxSongs(row.getInteger("max_songs"));
        s.setStreamQualityKbps(row.getInteger("stream_quality_kbps"));
        s.setDjTypeId(row.getUUID("dj_type_id"));
        s.setSupportLevel(row.getShort("support_level"));
        s.setCustomScriptAllowed(Boolean.TRUE.equals(row.getBoolean("custom_script_allowed")));
        return s;
    }
}
