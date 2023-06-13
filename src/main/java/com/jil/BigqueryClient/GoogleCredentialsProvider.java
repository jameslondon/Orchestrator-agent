package com.jil.BigqueryClient;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.IOException;

public class GoogleCredentialsProvider {

    private static GoogleCredentialsProvider instance = null;
    private GoogleCredentials credentials;

    private GoogleCredentialsProvider() {
//        try {
//            String keyPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
//            if (keyPath == null || keyPath.isEmpty()) {
//                System.out.println("Please set the GOOGLE_APPLICATION_CREDENTIALS environment variable");
//            } else {
//                credentials = ServiceAccountCredentials.fromStream(new FileInputStream(keyPath));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        try {
            credentials = ServiceAccountCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized GoogleCredentialsProvider getInstance() {
        if (instance == null) {
            instance = new GoogleCredentialsProvider();
        }
        return instance;
    }

    public GoogleCredentials getCredentials() {
        return this.credentials;
    }
}
