package com.newswatch.ContentAnalysisService.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.JsonData;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.*;

@Service
public class KeywordDataIndexingService {

    private final ElasticsearchClient client;

    @Autowired
    public KeywordDataIndexingService(ElasticsearchClient client) {
        this.client = client;
    }
    /**
     * Indexes a sorted map into Elasticsearch.
     *
     * @param indexName  The name of the Elasticsearch index.
     * @param sortedMap  The map containing date to integer mappings.
     * @param keyword    The keyword associated with the data.
     * @throws IOException, InterruptedException, ExecutionException If there's an issue indexing the data.
     */

    public void indexSortedMap(String fromIndex, Map<LocalDate, Integer> sortedMap, String keyword, String toIndex) throws IOException, InterruptedException {

        // Set a fixed upper limit for the thread pool size
        int numThreads = Math.min(10, Runtime.getRuntime().availableProcessors()); // Adjust the upper limit as necessary
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
//        System.out.println("IndexName: " + indexName);

        try {
            for (Map.Entry<LocalDate, Integer> entry : sortedMap.entrySet()) {
                executorService.submit(() -> {
                    try {
                        LocalDate date = entry.getKey();
                        Integer value = entry.getValue();

                        JsonObject jsonObject = Json.createObjectBuilder()
                                .add("date", date.toString())
                                .add("value", value)
                                .add("keyword", keyword)
                                .add("index", fromIndex)
                                .build();

                        Reader input = new StringReader(jsonObject.toString().replace('\'', '"'));
                        IndexRequest<JsonData> request = IndexRequest.of(i -> i.index(toIndex).withJson(input));
                        client.index(request);
                    } catch (Exception e) {
                        // Handle exceptions here
                        e.printStackTrace();
                    }
                });
            }
        } finally {
            executorService.shutdown();
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) { // Wait for all tasks to complete
                executorService.shutdownNow();
            }
//            client.close();
        }
    }
}
