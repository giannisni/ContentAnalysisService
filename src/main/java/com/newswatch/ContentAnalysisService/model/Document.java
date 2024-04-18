package com.newswatch.ContentAnalysisService.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class Document {
    private String title;
    private String text;



    private String summary;
    public String getDocument() {
        return document;
    }

    public void setDocument(String document) {
        this.document = document;
    }

    private String document;

    private List<String> authors;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    private String topic;
    @JsonProperty("published_date")
    private String publishedDate;
    private String url;



    private String tone;
    // Getters and setters for each field
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getText() {
        return text;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }
    public void setText(String text) {
        this.text = text;
    }

    public List<String> getAuthors() {
        return authors;
    }

    public void setAuthors(List<String> authors) {
        this.authors = authors;
    }
    public String getTone() {
        return tone;
    }

    public void setTone(String tone) {
        this.tone = tone;
    }
    public String getPublishedDate() {
        return publishedDate;
    }

    public void setPublishedDate(String publishedDate) {
        this.publishedDate = publishedDate;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
