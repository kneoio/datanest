package com.semantyca.datanest.mcp;

import io.quarkiverse.mcp.server.TextContent;
import io.quarkiverse.mcp.server.Tool;
import io.quarkiverse.mcp.server.ToolArg;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class KnowledgeTools {

    private static final Logger LOGGER = Logger.getLogger(KnowledgeTools.class);
    private static final String KNOWLEDGE_PATH = "knowledge/";

    private final Map<String, KnowledgeDoc> docs = new LinkedHashMap<>();

    @PostConstruct
    void loadDocs() {
        List<String> files = List.of("mixpla.md", "datanest.md");
        for (String file : files) {
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(KNOWLEDGE_PATH + file)) {
                if (is == null) {
                    LOGGER.warnf("Knowledge file not found: %s", file);
                    continue;
                }
                String raw = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                        .lines().collect(Collectors.joining("\n"));
                KnowledgeDoc doc = parse(raw);
                docs.put(doc.id(), doc);
                LOGGER.infof("Loaded knowledge doc: %s", doc.id());
            } catch (Exception e) {
                LOGGER.errorf("Failed to load knowledge file %s: %s", file, e.getMessage());
            }
        }
    }

    @Tool(description = "List all available knowledge documents with their id, title, summary, and tags.")
    TextContent list_docs() {
        StringBuilder sb = new StringBuilder();
        for (KnowledgeDoc doc : docs.values()) {
            sb.append("id: ").append(doc.id()).append("\n");
            sb.append("title: ").append(doc.title()).append("\n");
            sb.append("summary: ").append(doc.summary()).append("\n");
            sb.append("tags: ").append(String.join(", ", doc.tags())).append("\n\n");
        }
        return new TextContent(sb.toString().trim());
    }

    @Tool(description = "Search knowledge docs by keyword. Returns matching doc excerpts.")
    TextContent search_knowledge(@ToolArg(description = "Search query") String query) {
        String q = query.toLowerCase();
        StringBuilder sb = new StringBuilder();
        for (KnowledgeDoc doc : docs.values()) {
            if (doc.content().toLowerCase().contains(q) || doc.title().toLowerCase().contains(q)) {
                sb.append("## ").append(doc.title()).append(" (id: ").append(doc.id()).append(")\n");
                String[] lines = doc.content().split("\n");
                for (String line : lines) {
                    if (line.toLowerCase().contains(q)) {
                        sb.append("> ").append(line.trim()).append("\n");
                    }
                }
                sb.append("\n");
            }
        }
        return new TextContent(sb.isEmpty() ? "No results found for: " + query : sb.toString().trim());
    }

    @Tool(description = "Get the full content of a knowledge document by its id.")
    TextContent get_doc(@ToolArg(description = "Document id (from list_docs)") String id) {
        KnowledgeDoc doc = docs.get(id);
        if (doc == null) {
            return new TextContent("Document not found: " + id + ". Use list_docs() to see available ids.");
        }
        return new TextContent(doc.content());
    }

    private KnowledgeDoc parse(String raw) {
        String id = "", title = "", summary = "";
        List<String> tags = List.of();
        String content = raw;

        if (raw.startsWith("---")) {
            int end = raw.indexOf("---", 3);
            if (end > 0) {
                String frontmatter = raw.substring(3, end).trim();
                content = raw.substring(end + 3).trim();
                for (String line : frontmatter.split("\n")) {
                    if (line.startsWith("id:")) id = line.substring(3).trim();
                    else if (line.startsWith("title:")) title = line.substring(6).trim();
                    else if (line.startsWith("summary:")) summary = line.substring(8).trim();
                    else if (line.startsWith("tags:")) {
                        String t = line.substring(5).trim().replaceAll("[\\[\\]]", "");
                        tags = Arrays.stream(t.split(",")).map(String::trim).toList();
                    }
                }
            }
        }
        return new KnowledgeDoc(id, title, summary, tags, content);
    }

    private record KnowledgeDoc(String id, String title, String summary, List<String> tags, String content) {}
}
