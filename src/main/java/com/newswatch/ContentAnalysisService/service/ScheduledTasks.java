package com.newswatch.ContentAnalysisService.service;

import com.newswatch.ContentAnalysisService.model.KeywordResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;

@Service
public class ScheduledTasks {

    @Autowired
    private WordFrequencyBatch wordFrequencyBatch;

    @Autowired
    private KeywordDataIndexingService indexMap;

    public ScheduledTasks(WordFrequencyBatch wordFrequencyBatch) {
        this.wordFrequencyBatch = wordFrequencyBatch;
    }

//        @Scheduled(cron = "0 53  21 * * *")

    public void scheduleKeywordCountTask(List<String> keywords, String fromIndex, String toIndex, String startDate, String endDate ) {


        System.out.println("Index = " + fromIndex);
        System.out.println("Keywords = " + keywords);

        System.out.println("EndIndex = " + toIndex);
        System.out.println("startDate =  " + startDate);
        System.out.println("endDate =  " + endDate);
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        List<Future<KeywordResult>> futures = new ArrayList<>();

        for (String keyword : keywords) {
            final String currentKeyword = keyword;
            Callable<KeywordResult> task = () -> {
                try {
//                    System.out.println("Keyword " + keyword );
                    Map<LocalDate, Integer> resultMap = wordFrequencyBatch.searchKeywordFrequency(fromIndex, toIndex, currentKeyword, startDate, endDate);

                    return new KeywordResult(currentKeyword, resultMap);
                } catch (Exception e) {
//                    logger.error("Error processing keyword: " + currentKeyword, e);
                    return new KeywordResult(currentKeyword, Collections.emptyMap());
                }
            };
            futures.add(executorService.submit(task));
        }

        for (Future<KeywordResult> future : futures) {
            try {
                KeywordResult result = future.get();
                Map<LocalDate, Integer> resultMap = result.getFrequencyMap();
                if (!resultMap.isEmpty()) {


                    indexMap.indexSortedMap(fromIndex, new TreeMap<>(resultMap), result.getKeyword(), toIndex);
                }
//                logger.info("Result for keyword '" + result.getKeyword() + "': " + resultMap);
            } catch (InterruptedException | ExecutionException e) {
//                logger.error("Error retrieving keyword count result", e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        executorService.shutdown();
//        logger.info("Finished scheduled task for keyword count.");
    }
}