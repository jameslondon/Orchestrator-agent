package com.jil.BigqueryClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

public class BigQueryJSONLoader {
    public static Boolean loadGCSFromJSON(GoogleCredentials credentials,
                                          String bucketName,
                                          String blobName,
                                          String jsonStrings
                                          ) throws JsonProcessingException {
        if (credentials == null) {
            System.out.println("credentials is null");
            return false;
        }
        System.out.println("Start to load json response to GCS.");

        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();

        BlobId blobId = BlobId.of(bucketName, blobName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/json").build();

        ObjectMapper objectMapper = new ObjectMapper();
        // Convert JSON string to JsonNode
        JsonNode jsonNode = objectMapper.readTree(jsonStrings);
        // Get "payload" property
        JsonNode payload = jsonNode.get("payload");
        if (payload == null) {
            System.out.println("JSON string does not contain a 'payload' property.");
            return false;
        }

        // Convert "payload" back to JSON string
        //String jsonPayloadStr = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
        String jsonPayloadStr = objectMapper.writeValueAsString(payload);
        System.out.println("payload json: " + jsonPayloadStr);

        try (WritableByteChannel channel = storage.writer(blobInfo)) {
            channel.write(java.nio.ByteBuffer.wrap(jsonPayloadStr.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("JSON string successfully loaded into GCS.");
        return true;
    }

    public static void loadBQFromGCS(GoogleCredentials credentials,
                                     String bucketName,
                                     String blobName,
                                     String datasetName,
                                     String tableName) throws InterruptedException {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        TableId tableId = TableId.of(datasetName, tableName);

//        Table table = bigquery.getTable(tableId);
//        // if table already exist, then delete first
//        if (table != null) {
//            // Delete the table
//            boolean success = bigquery.delete(datasetName, tableName);
//            if (success) {
//                System.out.println("Table deleted successfully");
//            } else {
//                System.out.println("Table deletion failed");
//            }
//        } else {
//            System.out.println("Table does not exist");
//        }

        FormatOptions formatOptions = FormatOptions.json();
        LoadJobConfiguration loadJobConfig = LoadJobConfiguration.newBuilder(
                        tableId,
                        "gs://" + bucketName + "/" + blobName,
                        formatOptions)
                .setSchemaUpdateOptions(
                  ImmutableList.of(JobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION)) // Allow adding new fields
                .setWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND) // This appends to the table
                .setAutodetect(true)
                .build();

        Job loadJob = bigquery.create(JobInfo.newBuilder(loadJobConfig).build());
        loadJob = loadJob.waitFor();

        if (loadJob.isDone() && loadJob.getStatus().getError() == null) {
            System.out.println("JSON string successfully loaded into BigQuery table.");
        } else {
            System.out.println("Error occurred while loading JSON string into BigQuery table.");
        }
    }
}
