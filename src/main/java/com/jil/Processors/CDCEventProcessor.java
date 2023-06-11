package com.jil.Processors;

import com.jil.BigqueryClient.BigQueryJSONLoader;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.*;
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
public class CDCEventProcessor {
    GoogleCredentialsProvider credentials;
    ExecutorService singleThreadExecutor;
    BlockingQueue<String> queue;
    public CDCEventProcessor(
            GoogleCredentialsProvider credentials,
            ExecutorService singleThreadExecutor,
            BlockingQueue<String> queue

            ) {
        this.credentials = credentials;
        this.singleThreadExecutor = singleThreadExecutor;
        this.queue = queue;
    }


}
