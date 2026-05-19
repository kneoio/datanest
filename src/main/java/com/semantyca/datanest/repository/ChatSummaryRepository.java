package com.semantyca.datanest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.model.cnst.ChatType;
import com.semantyca.core.model.cnst.SummaryType;
import com.semantyca.core.repository.AsyncRepository;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.rls.RLSRepository;
import com.semantyca.datanest.model.ChatSummary;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class ChatSummaryRepository extends AsyncRepository {

    private static final String TABLE_NAME = "mixpla__chat_summaries";

    @Inject
    public ChatSummaryRepository(Pool client, ObjectMapper mapper, RLSRepository rlsRepository) {
        super(client, mapper, rlsRepository);
    }

    public Uni<List<ChatSummary>> getAll(int limit, int offset) {
        String sql = "SELECT * FROM " + TABLE_NAME + " ORDER BY created_at DESC";

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
        String sql = "SELECT COUNT(*) FROM " + TABLE_NAME;
        return client.query(sql)
                .execute()
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<ChatSummary> findById(UUID id) {
        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(RowSet::iterator)
                .onItem().transform(iterator -> {
                    if (iterator.hasNext()) return from(iterator.next());
                    throw new DocumentHasNotFoundException(id);
                });
    }

    public Uni<List<ChatSummary>> findByBrandName(String brandName, int limit, int offset) {
        String sql = "SELECT * FROM " + TABLE_NAME + " WHERE brand_name = $1 ORDER BY created_at DESC";

        if (limit > 0) {
            sql += String.format(" LIMIT %s OFFSET %s", limit, offset);
        }

        return client.preparedQuery(sql)
                .execute(Tuple.of(brandName))
                .onItem().transformToMulti(rows -> Multi.createFrom().iterable(rows))
                .onItem().transform(this::from)
                .collect().asList();
    }

    public Uni<Integer> getCountByBrandName(String brandName) {
        String sql = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE brand_name = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(brandName))
                .onItem().transform(rows -> rows.iterator().next().getInteger(0));
    }

    public Uni<Integer> delete(UUID id) {
        String sql = "DELETE FROM " + TABLE_NAME + " WHERE id = $1";
        return client.preparedQuery(sql)
                .execute(Tuple.of(id))
                .onItem().transform(RowSet::rowCount);
    }

    private ChatSummary from(Row row) {
        ChatSummary chatSummary = new ChatSummary();
        chatSummary.setId(row.getUUID("id"));
        chatSummary.setBrandName(row.getString("brand_name"));
        chatSummary.setSummaryType(SummaryType.valueOf(row.getString("summary_type")));
        chatSummary.setUserId(row.getLong("user_id"));
        String chatTypeStr = row.getString("chat_type");
        if (chatTypeStr != null) {
            chatSummary.setChatType(ChatType.valueOf(chatTypeStr));
        }
        chatSummary.setSummary(row.getString("summary"));
        chatSummary.setMessageCount(row.getInteger("message_count"));
        chatSummary.setPeriodStart(row.getOffsetDateTime("period_start"));
        chatSummary.setPeriodEnd(row.getOffsetDateTime("period_end"));
        chatSummary.setCreatedAt(row.getOffsetDateTime("created_at"));
        return chatSummary;
    }
}
