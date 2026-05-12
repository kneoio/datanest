package com.semantyca.datanest.repository;

import com.semantyca.datanest.dto.RlsActionDTO;
import com.semantyca.datanest.dto.RlsActionType;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.Tuple;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public final class RlsActionUtil {

    private static final Logger LOGGER = Logger.getLogger(RlsActionUtil.class);

    private RlsActionUtil() {
    }

    public static Uni<Void> applyRlsActions(SqlClient tx, String rlsTable, UUID entityId, List<RlsActionDTO> actions) {
        LOGGER.infof("applyRlsActions: table=%s, entityId=%s, actions count=%s", rlsTable, entityId, actions.size());
        for (RlsActionDTO a : actions) {
            LOGGER.infof("  action=%s, userId=%s, canEdit=%s, canDelete=%s", a.getAction(), a.getUserId(), a.isCanEdit(), a.isCanDelete());
        }
        if (actions.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        String grantSql = String.format(
                "INSERT INTO %s (reader, entity_id, can_edit, can_delete) VALUES ($1, $2, $3, $4) " +
                "ON CONFLICT (reader, entity_id) DO UPDATE SET " +
                "can_edit = EXCLUDED.can_edit, can_delete = EXCLUDED.can_delete, reading_time = now()",
                rlsTable
        );
        String revokeSql = String.format(
                "DELETE FROM %s WHERE reader = $1 AND entity_id = $2",
                rlsTable
        );

        List<Uni<Void>> unis = new ArrayList<>();
        for (RlsActionDTO action : actions) {
            if (action.getAction() == RlsActionType.GRANT) {
                unis.add(tx.preparedQuery(grantSql)
                        .execute(Tuple.of(action.getUserId(), entityId, action.isCanEdit(), action.isCanDelete()))
                        .onItem().ignore().andContinueWithNull());
            } else if (action.getAction() == RlsActionType.REVOKE) {
                unis.add(tx.preparedQuery(revokeSql)
                        .execute(Tuple.of(action.getUserId(), entityId))
                        .onItem().ignore().andContinueWithNull());
            }
        }

        if (unis.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        return Uni.combine().all().unis(unis).discardItems();
    }

    /**
     * Grants access to a single user with OR-merge semantics on conflict:
     * existing permissions are never downgraded, only upgraded.
     */
    public static Uni<Void> grantMerge(SqlClient tx, String rlsTable, UUID entityId,
                                        long userId, boolean canEdit, boolean canDelete) {
        String sql = String.format(
                "INSERT INTO %s (reader, entity_id, can_edit, can_delete) VALUES ($1, $2, $3, $4) " +
                "ON CONFLICT (reader, entity_id) DO UPDATE SET " +
                "can_edit = %s.can_edit OR EXCLUDED.can_edit, " +
                "can_delete = %s.can_delete OR EXCLUDED.can_delete, " +
                "reading_time = now()",
                rlsTable, rlsTable, rlsTable
        );
        return tx.preparedQuery(sql)
                .execute(Tuple.of(userId, entityId, canEdit, canDelete))
                .replaceWithVoid();
    }

    /**
     * Grants access to whoever is stored in {@code authorColumn} of {@code sourceTable} for row {@code sourceId},
     * using OR-merge semantics so existing higher permissions are preserved.
     * Runs as a single INSERT … SELECT, no extra round-trip needed.
     */
    public static Uni<Void> grantFromAuthorColumn(SqlClient tx, String rlsTable, UUID entityId,
                                                   String sourceTable, UUID sourceId,
                                                   boolean canEdit, boolean canDelete) {
        String sql = String.format(
                "INSERT INTO %s (reader, entity_id, can_edit, can_delete) " +
                "SELECT author, $1, $2, $3 FROM %s WHERE id = $4 " +
                "ON CONFLICT (reader, entity_id) DO UPDATE SET " +
                "can_edit = %s.can_edit OR EXCLUDED.can_edit, " +
                "can_delete = %s.can_delete OR EXCLUDED.can_delete, " +
                "reading_time = now()",
                rlsTable, sourceTable, rlsTable, rlsTable
        );
        return tx.preparedQuery(sql)
                .execute(Tuple.of(entityId, canEdit, canDelete, sourceId))
                .replaceWithVoid();
    }

    /**
     * Grants access to the user stored as a JSON field inside {@code jsonColumn} of {@code sourceTable},
     * e.g. {@code (owner->>'userId')::bigint}. Uses OR-merge semantics.
     */
    public static Uni<Void> grantFromJsonField(SqlClient tx, String rlsTable, UUID entityId,
                                                String sourceTable, UUID sourceId,
                                                String jsonColumn, String jsonKey,
                                                boolean canEdit, boolean canDelete) {
        String sql = String.format(
                "INSERT INTO %s (reader, entity_id, can_edit, can_delete) " +
                "SELECT (%s->>'%s')::bigint, $1, $2, $3 FROM %s WHERE id = $4 " +
                "ON CONFLICT (reader, entity_id) DO UPDATE SET " +
                "can_edit = %s.can_edit OR EXCLUDED.can_edit, " +
                "can_delete = %s.can_delete OR EXCLUDED.can_delete, " +
                "reading_time = now()",
                rlsTable, jsonColumn, jsonKey, sourceTable, rlsTable, rlsTable
        );
        return tx.preparedQuery(sql)
                .execute(Tuple.of(entityId, canEdit, canDelete, sourceId))
                .replaceWithVoid();
    }

    public static Uni<Void> revoke(SqlClient tx, String rlsTable, UUID entityId, long userId) {
        String sql = String.format("DELETE FROM %s WHERE entity_id = $1 AND reader = $2", rlsTable);
        return tx.preparedQuery(sql)
                .execute(Tuple.of(entityId, userId))
                .replaceWithVoid();
    }

    public static Uni<Void> revokeFromJsonField(SqlClient tx, String rlsTable, UUID entityId,
                                                 String sourceTable, UUID sourceId,
                                                 String jsonColumn, String jsonKey) {
        String sql = String.format(
                "DELETE FROM %s WHERE entity_id = $1 AND reader = (SELECT (%s->>'%s')::bigint FROM %s WHERE id = $2)",
                rlsTable, jsonColumn, jsonKey, sourceTable
        );
        return tx.preparedQuery(sql)
                .execute(Tuple.of(entityId, sourceId))
                .replaceWithVoid();
    }
}
