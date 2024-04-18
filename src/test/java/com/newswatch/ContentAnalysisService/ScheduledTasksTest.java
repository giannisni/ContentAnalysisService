package com.newswatch.ContentAnalysisService;


import com.newswatch.ContentAnalysisService.service.ScheduledTasks;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ScheduledTasksTest {

    @Autowired
    private ScheduledTasks scheduledTasks;

    @Test
    public void testScheduleKeywordCountTask() {
        List<String> keywords = Arrays.asList("israeli", "israel", "palestine", "palestinian", "palestinians", "hamas", "israelis");
        String fromIndex = "cnn_articles_newone";
        String toIndex = "cnn_articles_newone_countsa";
        String startDate = "2024-02-01";
        String endDate = "2024-03-14";
        scheduledTasks.scheduleKeywordCountTask(keywords, fromIndex, toIndex, startDate, endDate);
    }


}
