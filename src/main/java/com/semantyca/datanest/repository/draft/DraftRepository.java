package com.semantyca.datanest.repository.draft;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.mixpla.model.Draft;
import com.semantyca.mixpla.model.filter.DraftFilter;
import com.semantyca.mixpla.repository.MixplaNameResolver;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static com.semantyca.mixpla.repository.MixplaNameResolver.DRAFT;


@ApplicationScoped
public class DraftRepository extends AsyncRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(DraftRepository.class);
    private static final EntityData entityData = MixplaNameResolver.create().getEntityNames(DRAFT);

    private final DraftQueryBuilder queryBuilder;

    @Inject
    public DraftRepository(Pool client, ObjectMapper mapper, DraftQueryBuilder queryBuilder) {
        super(client, mapper, null);
        this.queryBuilder = queryBuilder;
    }

    public Uni<List<Draft>> getAll(int limit, int offset, boolean includeArchived, final IUser user, final DraftFilter filter) {
        String sql = queryBuilder.buildGetAllQuery(
                entityData.getTableName(),
                includeArchived,
                filter,
                limit,
                offset
        );

        return client.query(sql)
                .execute()
                .onFailure().invoke(throwable -> LOGGER.error("Failed to retrieve drafts", throwable))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> getAllCount(IUser user, boolean includeArchived, final DraftFilter filter) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t";

        if (!includeArchived) {
            sql += " WHERE (t.archived IS NULL OR t.archived = 0)";
        }

        if (filter != null && filter.isActivated()) {
            sql += queryBuilder.buildFilterConditions(filter);
        }

        return client.query(sql)
                .execute()
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<Draft> findById(UUID id, IUser user, boolean includeArchived) {
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

    public Uni<Draft> insert(Draft draft, IUser user) {
        return Uni.createFrom().deferred(() -> {
            try {
                String sql = "INSERT INTO " + entityData.getTableName() +
                        " (author, reg_date, last_mod_user, last_mod_date, title, content, description, enabled, version) " +
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id";

                OffsetDateTime now = OffsetDateTime.now();

                Tuple params = Tuple.tuple()
                        .addLong(user.getId())
                        .addOffsetDateTime(now)
                        .addLong(user.getId())
                        .addOffsetDateTime(now)
                        .addString(draft.getTitle())
                        .addString(draft.getContent())
                        .addString(draft.getDescription())
                        .addBoolean(draft.isEnabled())
                        .addDouble(draft.getVersion());

                return client.preparedQuery(sql)
                        .execute(params)
                        .onItem().transform(result -> result.iterator().next().getUUID("id"))
                        .onItem().transformToUni(id -> findById(id, user, true));
            } catch (Exception e) {
                return Uni.createFrom().failure(e);
            }
        });
    }

    public Uni<Draft> update(UUID id, Draft draft, IUser user) {
        return Uni.createFrom().deferred(() -> {
            try {
                String sql = "UPDATE " + entityData.getTableName() +
                        " SET title = $1, content = $2, description = $3, enabled = $4, " +
                        "version = $5, last_mod_user = $6, last_mod_date = $7 WHERE id = $8";

                OffsetDateTime now = OffsetDateTime.now();

                Tuple params = Tuple.tuple()
                        .addString(draft.getTitle())
                        .addString(draft.getContent())
                        .addString(draft.getDescription())
                        .addBoolean(draft.isEnabled())
                        .addDouble(draft.getVersion())
                        .addLong(user.getId())
                        .addOffsetDateTime(now)
                        .addUUID(id);

                return client.preparedQuery(sql)
                        .execute(params)
                        .onItem().transformToUni(rowSet -> {
                            if (rowSet.rowCount() == 0) {
                                return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                            }
                            return findById(id, user, true);
                        });
            } catch (Exception e) {
                return Uni.createFrom().failure(e);
            }
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

    private Draft from(Row row) {
        Draft doc = new Draft();
        setDefaultFields(doc, row);

        doc.setTitle(row.getString("title"));
        doc.setContent(row.getString("content"));
        doc.setDescription(row.getString("description"));
        doc.setArchived(row.getInteger("archived"));
        doc.setEnabled(row.getBoolean("enabled"));

        doc.setVersion(row.getDouble("version"));

        return doc;
    }
}
