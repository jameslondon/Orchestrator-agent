
package com.jil;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auth.oauth2.GoogleCredentials;
import com.jil.BigqueryClient.BigQueryJSONLoader;
import com.jil.BigqueryClient.GoogleCredentialsProvider;
import com.jil.SFconnector.EmpConnector;
import com.jil.SFconnector.TopicSubscription;
import com.jil.util.BayeuxParameters;
import com.jil.util.nCinoAccess;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.*;

public class Main {
    // More than one thread can be used in the thread pool which leads to parallel processing of events which may be acceptable by the application
    // The main purpose of asynchronous event processing is to make sure that client is able to perform /meta/connect requests which keeps the session alive on the server side
    private static final ExecutorService workerThreadPool = Executors.newFixedThreadPool(1);

    public static void main(String[] argv) throws Exception {
        if (argv.length < 2 || argv.length > 6) {
            System.err.println("Usage: BearerTokenExample url token topic [replayFrom]");
            System.exit(1);
        }
        long replayFrom = EmpConnector.REPLAY_FROM_TIP;
        if (argv.length == 6) {
            replayFrom = Long.parseLong(argv[5]);
        }

        BayeuxParameters params = new BayeuxParameters() {

            @Override
            public String bearerToken() {
                //return AccessTokenFetcher.getAccessToken(argv[0]);
                return nCinoAccess.getAccessTokenSupplier().get().get("access_token");
            }

            ;

            @Override
            public URL host() {
                try {
                    return new URL(argv[0]);
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException(String.format("Unable to create url: %s", argv[0]), e);
                }
            }
        };

        //Consumer<Map<String, Object>> consumer = event -> workerThreadPool.submit(() -> System.out.println(String.format("Received:\n%s, \nEvent processed by threadName:%s, threadId: %s", JSON.toString(event), Thread.currentThread().getName(), Thread.currentThread().getId())));

        // Create ObjectMapper instance
        ObjectMapper mapper = new ObjectMapper();

        GoogleCredentials credentials = GoogleCredentialsProvider.getInstance().getCredentials();
        System.out.println("Google credentials: " + credentials);
        if (credentials == null) {
            System.out.println("Failed to get Google credentials.");
        }

        Consumer<Map<String, Object>> consumer = event -> {
            String blobName = "ncinoconnection.json";
            String bqTableName = "nCinoConnection";
            workerThreadPool.submit(() -> {
                        try {
                            String jsonStr = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(event);
                            System.out.println("Change Data Capture event fired. Data:\n" + jsonStr);
                            if (BigQueryJSONLoader.loadGCSFromJSON(credentials,
                                    "jianliu888-hometest",
                                    blobName,
                                    jsonStr)) {
                                BigQueryJSONLoader.loadBQFromGCS(credentials,
                                        "jianliu888-hometest",
                                        blobName,
                                        "jianliuhometest",
                                        bqTableName);
                            }
                        } catch (JsonProcessingException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        };
        Consumer<Map<String, Object>> consumerSecurity = event -> {
            String blobName = "ncinosecurity.json";
            String bqTableName = "nCinoSecurity";
            workerThreadPool.submit(() -> {
                        try {
                            String jsonStr = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(event);
                            System.out.println("Change Data Capture event fired. Data:\n" + jsonStr);
                            if (BigQueryJSONLoader.loadGCSFromJSON(credentials,
                                    "jianliu888-hometest",
                                    blobName,
                                    jsonStr)) {
                                BigQueryJSONLoader.loadBQFromGCS(credentials,
                                        "jianliu888-hometest",
                                        blobName,
                                        "jianliuhometest",
                                        bqTableName);
                            }
                        } catch (JsonProcessingException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        };

        Consumer<Map<String, Object>> consumerCovenant = event -> {
            String blobName = "ncinocovenant.json";
            String bqTableName = "nCinoCovenant";
            workerThreadPool.submit(() -> {
                        try {
                            String jsonStr = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(event);
                            System.out.println("Change Data Capture event fired. Data:\n" + jsonStr);
                            if (BigQueryJSONLoader.loadGCSFromJSON(credentials,
                                    "jianliu888-hometest",
                                    blobName,
                                    jsonStr)) {
                                BigQueryJSONLoader.loadBQFromGCS(credentials,
                                        "jianliu888-hometest",
                                        blobName,
                                        "jianliuhometest",
                                        bqTableName);
                            }
                        } catch (JsonProcessingException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        };

        EmpConnector connector = new EmpConnector(params);

        connector.start().get(5, TimeUnit.SECONDS);

        TopicSubscription subscription = connector.subscribe(argv[2], replayFrom, consumer).get(5, TimeUnit.SECONDS);
        TopicSubscription subscriptionSecurity = connector.subscribe(argv[3], replayFrom, consumerSecurity).get(5, TimeUnit.SECONDS);
        TopicSubscription subscriptionCovenant = connector.subscribe(argv[4], replayFrom, consumerCovenant).get(5, TimeUnit.SECONDS);


        System.out.println(String.format("Subscribed: %s", subscription));
        System.out.println(String.format("Subscribed: %s", subscriptionSecurity));
        System.out.println(String.format("Subscribed: %s", subscriptionCovenant));
    }
}
