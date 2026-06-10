package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.UserSubscription;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.table.TableNameResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

@ApplicationScoped
public class UserSubscriptionRepository extends AsyncRepository {

    private static final String TABLE = TableNameResolver.create()
            .getEntityNames(TableNameResolver.USER_SUBSCRIPTION_ENTITY_NAME)
            .getTableName();

    @Inject
    public UserSubscriptionRepository(Pool client, ObjectMapper mapper) {
        super(client, mapper, null);
    }

    public Uni<List<UserSubscription>> findByUserId(Long userId) {
        String sql = "SELECT * FROM " + TABLE + " WHERE user_id = $1 ORDER BY reg_date DESC";
        return client.preparedQuery(sql)
                .execute(Tuple.of(userId))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    private UserSubscription from(Row row) {
        UserSubscription s = new UserSubscription();
        s.setId(row.getUUID("id"));
        s.setUserId(row.getLong("user_id"));
        s.setStripeSubscriptionId(row.getString("stripe_subscription_id"));
        s.setSubscriptionType(row.getString("subscription_type"));
        s.setSubscriptionStatus(row.getString("subscription_status"));
        var trialEnd = row.getOffsetDateTime("trial_end");
        if (trialEnd != null) s.setTrialEnd(trialEnd.toZonedDateTime());
        s.setActive(Boolean.TRUE.equals(row.getBoolean("active")));
        return s;
    }
}
