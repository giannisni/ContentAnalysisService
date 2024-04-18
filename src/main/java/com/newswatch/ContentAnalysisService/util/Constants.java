package com.newswatch.ContentAnalysisService.util;

public class Constants {
    public static final String elastic_host;

    public static final String ELASTICSEARCH_USERNAME = System.getenv("ELASTICSEARCH_USERNAME");
    public static final String ELASTICSEARCH_PASSWORD = System.getenv("ELASTICSEARCH_PASSWORD");
    public static final int ELASTICSEARCH_PORT = Integer.parseInt(System.getenv("ELASTICSEARCH_PORT"));

    public static final String ELASTICSEARCH_PROTOCOL =  System.getenv("ELASTICSEARCH_PROTOCOL");

    static {


        // Fetching the ELASTICSEARCH_HOST environment variable
        String host = System.getenv("ELASTICSEARCH_HOST");
        if (host != null && !host.isEmpty()) {

            elastic_host = host;
        } else {
            // Default value if the environment variable is not set
            elastic_host = "localhost";
        }
    }
}
