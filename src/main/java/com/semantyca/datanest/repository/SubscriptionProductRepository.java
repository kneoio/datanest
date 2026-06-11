package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.SubscriptionProduct;
import com.semantyca.core.repository.AsyncRepository;
import io.vertx.core.json.JsonObject;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.core.repository.table.TableNameResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;

import static com.semantyca.core.repository.table.TableNameResolver.SUBSCRIPTION_PRODUCT_ENTITY_NAME;

@ApplicationScoped
public class SubscriptionProductRepository extends AsyncRepository {

    private static final EntityData entityData = TableNameResolver.create().getEntityNames(SUBSCRIPTION_PRODUCT_ENTITY_NAME);

    @Inject
    public SubscriptionProductRepository(Pool client, ObjectMapper mapper) {
        super(client, mapper, null);
    }

    public Uni<List<SubscriptionProduct>> getAll(int limit, int offset) {
        String sql = String.format("SELECT * FROM %s ORDER BY order_number DESC", entityData.getTableName());
        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }
        return client.query(sql)
                .execute()
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> getAllCount() {
        return getAllCount(entityData.getTableName());
    }

    public Uni<SubscriptionProduct> findById(UUID uuid) {
        return findById(uuid, entityData, this::from);
    }

    private SubscriptionProduct from(Row row) {
        SubscriptionProduct doc = new SubscriptionProduct();
        setDefaultFields(doc, row);
        doc.setIdentifier(row.getString("identifier"));
        doc.setLocalizedName(getLocName(row));
        doc.setStripePriceId(row.getString("stripe_price_id"));
        doc.setStripeProductId(row.getString("stripe_product_id"));
        doc.setLocalizedDescription(getLocData(row, "loc_descr"));
        Boolean active = row.getBoolean("active");
        doc.setActive(active != null ? active : true);
        JsonObject dv = row.getJsonObject("default_values");
        doc.setDefaultValues(dv != null ? dv.getMap() : new java.util.HashMap<>());
        return doc;
    }
}
