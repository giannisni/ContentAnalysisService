package com.newswatch.ContentAnalysisService.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.json.JsonData;
import com.newswatch.ContentAnalysisService.model.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

@Service
public class WordFrequencyBatch {
    private static AtomicInteger processedHits = new AtomicInteger(0);

    private final ElasticsearchClient client;

    @Autowired
    public WordFrequencyBatch(ElasticsearchClient client) {
        this.client = client;
    }

    private static final Logger logger = Logger.getLogger(WordFrequencyBatch.class.getName());

   
    /**
     * Extracts a LocalDate from an object, using the provided formatter.
     *
     * @param publishedDateObj The object containing the date.
     * @param formatter        The formatter to use for parsing.
     * @return The extracted LocalDate, or null if extraction is not possible.
     */
    private static LocalDate extractDate(Object publishedDateObj, DateTimeFormatter formatter) {
        if (publishedDateObj instanceof String) {
            return LocalDate.parse(publishedDateObj.toString(), formatter);
        } else if (publishedDateObj instanceof List) {
            List<String> publishedDateList = (List<String>) publishedDateObj;
            if (!publishedDateList.isEmpty()) {
                return LocalDate.parse(publishedDateList.get(0), formatter);
            }
        }
        return null;
    }

    /**
     * Counts the frequency of each keyword in the provided content.
     *
     * @param content  The content to search within.
     * @param keywords The keywords to count.
     * @return The total frequency of all keywords.
     */
    private static int countKeywordFrequency(String content, List<String> keywords) {
        int frequency = 0;

        String contentLower = content.toLowerCase(); // Convert content to lower case

        for (String keyword : keywords) {
            String keywordLower = keyword.toLowerCase(); // Convert keyword to lower case
            frequency += (contentLower.split(keywordLower, -1).length) - 1;
        }
//        System.out.print("Frequency : " + frequency);

        return frequency;
    }



    /**
     * Searches for the frequency of a given keyword within a specified date range in an Elasticsearch index.
     *
     * @param indexName  The name of the Elasticsearch index to search.
     * @param keyword    The keyword to search for.
     * @param startDate  The start date of the search range.
     * @param endDate    The end date of the search range.
     * @return A sorted map with dates as keys and keyword frequencies as values.
     */


    public Map<LocalDate, Integer> searchKeywordFrequency(String indexName,String toIndex, String keyword, String startDate, String endDate) throws IOException, InterruptedException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        Map<LocalDate, Integer> dateFrequencyMap = new ConcurrentHashMap<>();



        try {
            // Preparing the search request
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(indexName)
                    .query(q -> q
                            .bool(b -> b
                                    .must(m -> m
                                            .match(t -> t
                                                    .field("text")
                                                    .query(keyword)
                                            )
                                    )
                                    .filter(f -> f
                                            .range(r -> r
                                                    .field("published_date")
                                                    .gte(JsonData.of(startDate))
                                                    .lte(JsonData.of(endDate))
                                            )
                                    )
                            )
                    )
                    .size(1000) // Adjust batch size as needed
                    .build();


            SearchResponse<Document> searchResponse = client.search(searchRequest, Document.class);
//            System.out.println("searchResponse = " + searchResponse);


            HitsMetadata<Document> hits = searchResponse.hits();
//            System.out.println("Hits: " + Arrays.toString(hits.hits().toArray()));

            ExecutorService executorService = Executors.newFixedThreadPool(4);
            try {
                for (Hit<Document> hit : hits.hits()) {
                    executorService.submit(() -> {
                        try {
                            Document doc = hit.source();
                            if (doc != null && doc.getText() != null) {
                               // LocalDate date = LocalDate.parse(doc.getPublishedDate(), formatter);
                                String datePart = doc.getPublishedDate().split("T")[0]; // This gets only the date part
                                LocalDate dates = LocalDate.parse(datePart, DateTimeFormatter.ISO_LOCAL_DATE);

                                int frequency = countKeywordFrequency(doc.getText(), Collections.singletonList(keyword));

                                dateFrequencyMap.merge(dates, frequency, Integer::sum);
                            } else {
                                System.out.println("Document or text is null.");
                            }
                        } catch (Exception e) {
                            e.printStackTrace(); // This will print any exception that occurs within the lambda
                        }
                    });
                }
            } finally {
                executorService.shutdown();
                try {
                    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                        executorService.shutdownNow(); // Cancel currently executing tasks
                        if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
                            System.err.println("ExecutorService did not terminate");
                    }
                } catch (InterruptedException ie) {
                    executorService.shutdownNow();
                    Thread.currentThread().interrupt(); // Preserve interrupt status
                }
            }

        } catch (Exception e) {  // Catching a general Exception to capture any type of error
            e.printStackTrace(); // Log the stack trace for debugging
        }



        return new TreeMap<>(dateFrequencyMap);
    }}