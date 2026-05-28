package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.mixpla.model.UserAd;
import com.semantyca.mixpla.model.filter.UserAdFilter;
import com.semantyca.mixpla.repository.MixplaNameResolver;
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

import static com.semantyca.mixpla.repository.MixplaNameResolver.USER_AD;

@ApplicationScoped
public class UserAdRepository extends AsyncRepository {
    private static final EntityData entityData = MixplaNameResolver.create().getEntityNames(USER_AD);

    @Inject
    public UserAdRepository(Pool client, ObjectMapper mapper) {
        super(client, mapper, null);
    }

    public Uni<List<UserAd>> getAll(int limit, int offset, boolean includeArchived, IUser user, UserAdFilter filter) {
        String sql = buildListQuery(includeArchived, filter);
        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }
        return client.query(sql)
                .execute()
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> getAllCount(IUser user, boolean includeArchived, UserAdFilter filter) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t";
        sql += buildWhereClause(includeArchived, filter);
        return client.query(sql)
                .execute()
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<UserAd> findById(UUID id, boolean includeArchived) {
        String sql = "SELECT * FROM " + entityData.getTableName() + " WHERE id = $1";
        if (!includeArchived) {
            sql += " AND archived = 0";
        }
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(RowSet::iterator)
                .onItem().transformToUni(iterator -> {
                    if (iterator.hasNext()) {
                        return Uni.createFrom().item(from(iterator.next()));
                    } else {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                    }
                });
    }

    public Uni<UserAd> insert(UserAd entity, IUser user) {
        String sql = "INSERT INTO " + entityData.getTableName() +
                " (author, reg_date, last_mod_user, last_mod_date, user_id, title, description, contacts) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id";
        OffsetDateTime now = OffsetDateTime.now();
        Tuple params = Tuple.tuple()
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addLong(entity.getUserId())
                .addString(entity.getTitle())
                .addString(entity.getDescription())
                .addString(entity.getContacts());
        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transform(result -> result.iterator().next().getUUID("id"))
                .onItem().transformToUni(id -> findById(id, true));
    }

    public Uni<UserAd> update(UUID id, UserAd entity, IUser user) {
        String sql = "UPDATE " + entityData.getTableName() +
                " SET title = $1, description = $2, contacts = $3, last_mod_user = $4, last_mod_date = $5 WHERE id = $6";
        OffsetDateTime now = OffsetDateTime.now();
        Tuple params = Tuple.tuple()
                .addString(entity.getTitle())
                .addString(entity.getDescription())
                .addString(entity.getContacts())
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addUUID(id);
        return client.preparedQuery(sql)
                .execute(params)
                .onItem().transformToUni(rowSet -> {
                    if (rowSet.rowCount() == 0) {
                        return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                    }
                    return findById(id, true);
                });
    }

    public Uni<Integer> archive(UUID id, IUser user) {
        String sql = "UPDATE " + entityData.getTableName() +
                " SET archived = 1, last_mod_date = $1, last_mod_user = $2 WHERE id = $3";
        OffsetDateTime now = OffsetDateTime.now();
        return client.preparedQuery(sql)
                .execute(Tuple.of(now, user.getId(), id))
                .onItem().transform(RowSet::rowCount);
    }

    public Uni<Integer> delete(UUID id) {
        String sql = "DELETE FROM " + entityData.getTableName() + " WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(RowSet::rowCount);
    }

    private String buildListQuery(boolean includeArchived, UserAdFilter filter) {
        return "SELECT * FROM " + entityData.getTableName() + " t" +
                buildWhereClause(includeArchived, filter) +
                " ORDER BY t.reg_date DESC";
    }

    private String buildWhereClause(boolean includeArchived, UserAdFilter filter) {
        StringBuilder sb = new StringBuilder();
        if (!includeArchived) {
            sb.append(" WHERE (t.archived IS NULL OR t.archived = 0)");
        }
        if (filter != null && filter.isActivated()) {
            String prefix = sb.length() == 0 ? " WHERE " : " AND ";
            if (filter.getUserId() != null) {
                sb.append(prefix).append("t.user_id = ").append(filter.getUserId());
                prefix = " AND ";
            }
            if (filter.getSearchTerm() != null && !filter.getSearchTerm().isEmpty()) {
                sb.append(prefix).append("t.search_keywords LIKE '%")
                        .append(filter.getSearchTerm().toLowerCase().replace("'", "''"))
                        .append("%'");
            }
        }
        return sb.toString();
    }

    private UserAd from(Row row) {
        UserAd doc = new UserAd();
        setDefaultFields(doc, row);
        doc.setUserId(row.getLong("user_id"));
        doc.setTitle(row.getString("title"));
        doc.setDescription(row.getString("description"));
        doc.setContacts(row.getString("contacts"));
        doc.setArchived(row.getInteger("archived"));
        return doc;
    }
}
