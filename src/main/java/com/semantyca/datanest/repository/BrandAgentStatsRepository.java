package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.mixpla.model.BrandAgentStats;
import com.semantyca.mixpla.model.filter.BrandAgentStatsFilter;
import com.semantyca.mixpla.repository.MixplaNameResolver;
import com.semantyca.officeframe.model.cnst.CountryCode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;

import static com.semantyca.mixpla.repository.MixplaNameResolver.BRAND_STATS;

@ApplicationScoped
public class BrandAgentStatsRepository extends AsyncRepository {

    private static final String TABLE_NAME = MixplaNameResolver.create()
            .getEntityNames(BRAND_STATS).getTableName();

    @Inject
    public BrandAgentStatsRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository) {
        super(client, mapper, rlsRepository);
    }

    public Uni<List<BrandAgentStats>> getAll(int limit, int offset, BrandAgentStatsFilter filter) {
        String sql = "SELECT * FROM " + TABLE_NAME;

        if (filter != null && filter.isActivated()) {
            sql += buildFilterConditions(filter);
        }

        sql += " ORDER BY last_access_time DESC";

        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }

        return client.query(sql)
                .execute()
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> getAllCount(BrandAgentStatsFilter filter) {
        String sql = "SELECT COUNT(*) FROM " + TABLE_NAME;

        if (filter != null && filter.isActivated()) {
            sql += buildFilterConditions(filter);
        }

        return client.query(sql)
                .execute()
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<BrandAgentStats> findById(Integer id) {
        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(rows -> {
                    var iterator = rows.iterator();
                    if (iterator.hasNext()) return from(iterator.next());
                    throw new DocumentHasNotFoundException(id.toString());
                });
    }

    public Uni<Integer> delete(Integer id) {
        String sql = "DELETE FROM " + TABLE_NAME + " WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(RowSet::rowCount);
    }

    private String buildFilterConditions(BrandAgentStatsFilter filter) {
        StringBuilder sb = new StringBuilder(" WHERE 1=1");

        if (filter.getStationName() != null && !filter.getStationName().isEmpty()) {
            sb.append(" AND station_name ILIKE '%")
              .append(filter.getStationName().replace("'", "''"))
              .append("%'");
        }

        if (filter.getCountryCode() != null) {
            sb.append(" AND country_code = '")
              .append(filter.getCountryCode().name())
              .append("'");
        }

        if (filter.getStreamType() != null) {
            sb.append(" AND stream_type = '")
              .append(filter.getStreamType())
              .append("'");
        }

        return sb.toString();
    }

    private BrandAgentStats from(Row row) {
        BrandAgentStats stats = new BrandAgentStats();
        stats.setId(row.getInteger("id"));
        stats.setStationName(row.getString("station_name"));
        stats.setUserAgent(row.getString("user_agent"));
        stats.setIpAddress(row.getString("ip_address"));
        String cc = row.getString("country_code");
        if (cc != null) {
            stats.setCountryCode(CountryCode.valueOf(cc));
        }
        stats.setAccessCount(row.getLong("access_count"));
        stats.setStreamType(row.getString("stream_type"));
        stats.setLastAccessTime(row.getOffsetDateTime("last_access_time"));
        return stats;
    }
}
