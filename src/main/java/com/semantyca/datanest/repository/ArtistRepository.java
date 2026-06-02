package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.UserData;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.embedded.DocumentAccessInfo;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.exception.DocumentModificationAccessException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.core.repository.rls.RlsActionUtil;
import com.semantyca.core.repository.table.EntityData;
import com.semantyca.datanest.model.artist.Artist;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.*;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class ArtistRepository extends AsyncRepository {
    private static final EntityData entityData = new EntityData("mixpla__artists", "mixpla__artist_readers");

    public ArtistRepository() {
        super();
    }

    @Inject
    public ArtistRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository) {
        super(client, mapper, rlsRepository);
    }

    public Uni<List<Artist>> getAll(int limit, int offset, boolean includeArchived, IUser user) {
        String sql = "SELECT t.*, rls.* FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls " +
                "WHERE t.id = rls.entity_id AND rls.reader = " + user.getId();
        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }
        sql += " ORDER BY t.reg_date DESC";
        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }
        return client.query(sql).execute()
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transformToUni(this::from)
                .concatenate().collect().asList();
    }

    public Uni<Integer> getAllCount(IUser user, boolean includeArchived) {
        String sql = "SELECT COUNT(*) FROM " + entityData.getTableName() + " t, " + entityData.getRlsName() + " rls " +
                "WHERE t.id = rls.entity_id AND rls.reader = " + user.getId();
        if (!includeArchived) {
            sql += " AND t.archived = 0";
        }
        return client.query(sql).execute()
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<Artist> findById(UUID uuid, IUser user, boolean includeArchived) {
        String sql = "SELECT t.*, rls.* FROM " + entityData.getTableName() + " t " +
                "JOIN " + entityData.getRlsName() + " rls ON t.id = rls.entity_id " +
                "WHERE rls.reader = $1 AND t.id = $2";
        if (!includeArchived) {
            sql += " AND (t.archived IS NULL OR t.archived = 0)";
        }
        return client.preparedQuery(sql).execute(Tuple.of(user.getId(), uuid))
                .onItem().transform(RowSet::iterator)
                .onItem().transformToUni(it -> {
                    if (it.hasNext()) return from(it.next());
                    throw new DocumentHasNotFoundException(uuid);
                });
    }

    public Uni<Artist> insert(Artist doc, List<RlsActionDTO> rlsActions, IUser user) {
        OffsetDateTime now = ZonedDateTime.now(ZoneOffset.UTC).toOffsetDateTime();
        String sql = "INSERT INTO " + entityData.getTableName() +
                " (reg_date, author, last_mod_date, last_mod_user, archived, localized_name, country, styles, description, links, user_data) " +
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id";

        Tuple params = Tuple.tuple()
                .addOffsetDateTime(now)
                .addLong(user.getId())
                .addOffsetDateTime(now)
                .addLong(user.getId())
                .addInteger(0)
                .addValue(serializeLocalizedName(doc.getLocalizedName()))
                .addString(doc.getCountry())
                .addValue(serializeStyles(doc.getStyles()))
                .addString(doc.getDescription())
                .addValue(serializeLinks(doc.getLinks()))
                .addValue(serializeUserData(doc.getUserData()));

        return client.withTransaction(tx -> tx.preparedQuery(sql).execute(params)
                .onItem().transform(r -> r.iterator().next().getUUID("id"))
                .onItem().transformToUni(id ->
                        insertRLSPermissions(tx, id, entityData, user)
                                .onItem().transformToUni(ignored -> applyRlsActions(tx, id, rlsActions))
                                .onItem().transform(ignored -> id)
                )
        ).onItem().transformToUni(id -> findById(id, user, true));
    }

    public Uni<Artist> update(UUID id, Artist doc, List<RlsActionDTO> rlsActions, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[0]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException(
                                "User does not have edit permission", user.getUserName(), id));
                    }
                    OffsetDateTime now = ZonedDateTime.now(ZoneOffset.UTC).toOffsetDateTime();
                    String sql = "UPDATE " + entityData.getTableName() +
                            " SET last_mod_user=$1, last_mod_date=$2, localized_name=$3, country=$4, " +
                            "styles=$5, description=$6, links=$7, user_data=$8 WHERE id=$9";
                    Tuple params = Tuple.tuple()
                            .addLong(user.getId())
                            .addOffsetDateTime(now)
                            .addValue(serializeLocalizedName(doc.getLocalizedName()))
                            .addString(doc.getCountry())
                            .addValue(serializeStyles(doc.getStyles()))
                            .addString(doc.getDescription())
                            .addValue(serializeLinks(doc.getLinks()))
                            .addValue(serializeUserData(doc.getUserData()))
                            .addUUID(id);
                    return client.withTransaction(tx ->
                            tx.preparedQuery(sql).execute(params)
                                    .onItem().transformToUni(rowSet -> {
                                        if (rowSet.rowCount() == 0) {
                                            return Uni.createFrom().failure(new DocumentHasNotFoundException(id));
                                        }
                                        return applyRlsActions(tx, id, rlsActions);
                                    })
                    ).onItem().transformToUni(ignored -> findById(id, user, true));
                });
    }

    public Uni<Integer> archive(UUID id, IUser user) {
        return archive(id, entityData, user);
    }

    public Uni<Integer> delete(UUID id, IUser user) {
        return rlsRepository.findById(entityData.getRlsName(), user.getId(), id)
                .onItem().transformToUni(permissions -> {
                    if (!permissions[1]) {
                        return Uni.createFrom().failure(new DocumentModificationAccessException(
                                "User does not have delete permission", user.getUserName(), id));
                    }
                    return client.withTransaction(tx -> {
                        String deleteRls = String.format("DELETE FROM %s WHERE entity_id = $1", entityData.getRlsName());
                        String deleteDoc = String.format("DELETE FROM %s WHERE id = $1", entityData.getTableName());
                        return tx.preparedQuery(deleteRls).execute(Tuple.of(id))
                                .onItem().transformToUni(ignored -> tx.preparedQuery(deleteDoc).execute(Tuple.of(id)))
                                .onItem().transform(RowSet::rowCount);
                    });
                });
    }

    public Uni<List<DocumentAccessInfo>> getDocumentAccessInfo(UUID documentId, IUser user) {
        return getDocumentAccessInfo(documentId, entityData, user);
    }

    private Uni<Void> applyRlsActions(SqlClient tx, UUID entityId, List<RlsActionDTO> actions) {
        return RlsActionUtil.applyRlsActions(tx, entityData.getRlsName(), entityId, actions);
    }

    private Uni<Artist> from(Row row) {
        Artist doc = new Artist();
        setDefaultFields(doc, row);
        doc.setCountry(row.getString("country"));
        doc.setDescription(row.getString("description"));
        Integer archived = row.getInteger("archived");
        doc.setArchived(archived != null ? archived : 0);

        JsonObject localizedNameJson = row.getJsonObject("localized_name");
        if (localizedNameJson != null) {
            EnumMap<LanguageCode, String> map = new EnumMap<>(LanguageCode.class);
            localizedNameJson.getMap().forEach((k, v) -> {
                try {
                    map.put(LanguageCode.valueOf(k), (String) v);
                } catch (Exception ignored) {}
            });
            doc.setLocalizedName(map);
        }

        JsonObject stylesJson = row.getJsonObject("styles");
        if (stylesJson != null) {
            JsonArray items = stylesJson.getJsonArray("items");
            if (items != null) {
                doc.setStyles(items.stream().map(Object::toString).collect(Collectors.toList()));
            }
        }

        JsonObject linksJson = row.getJsonObject("links");
        if (linksJson != null) {
            Map<String, String> links = new HashMap<>();
            linksJson.getMap().forEach((k, v) -> {
                if (v instanceof String) links.put(k, (String) v);
            });
            doc.setLinks(links);
        }

        JsonObject userDataJson = row.getJsonObject("user_data");
        if (userDataJson != null) {
            Map<String, String> data = new HashMap<>();
            userDataJson.getMap().forEach((k, v) -> {
                if (v instanceof String) data.put(k, (String) v);
            });
            doc.setUserData(new UserData(data));
        }

        return Uni.createFrom().item(doc);
    }

    private JsonObject serializeLocalizedName(EnumMap<LanguageCode, String> localizedName) {
        if (localizedName == null || localizedName.isEmpty()) return null;
        JsonObject json = new JsonObject();
        localizedName.forEach((k, v) -> json.put(k.name(), v));
        return json;
    }

    private JsonObject serializeStyles(List<String> styles) {
        if (styles == null || styles.isEmpty()) return null;
        return new JsonObject().put("items", new JsonArray(styles));
    }

    private JsonObject serializeLinks(Map<String, String> links) {
        if (links == null || links.isEmpty()) return null;
        JsonObject json = new JsonObject();
        links.forEach(json::put);
        return json;
    }

    private JsonObject serializeUserData(UserData userData) {
        if (userData == null) return null;
        JsonObject json = new JsonObject();
        userData.getData().forEach(json::put);
        return json;
    }
}
