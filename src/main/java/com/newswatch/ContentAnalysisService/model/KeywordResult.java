package com.newswatch.ContentAnalysisService.model;

import java.time.LocalDate;
import java.util.Map;

public class KeywordResult {
    private String keyword;
    private Map<LocalDate, Integer> frequencyMap;

    public KeywordResult(String keyword, Map<LocalDate, Integer> frequencyMap) {
        this.keyword = keyword;
        this.frequencyMap = frequencyMap;
    }

    public String getKeyword() {
        return keyword;
    }

    public Map<LocalDate, Integer> getFrequencyMap() {
        return frequencyMap;
    }
}
