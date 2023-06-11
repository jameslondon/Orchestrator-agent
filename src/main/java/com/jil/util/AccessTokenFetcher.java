package com.jil.util;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;

public class AccessTokenFetcher {

    public static String getAccessToken(String endpoint) {
        String clientId = "3MVG9Lf04EwncL7mm2UlskTO40uIpz6IOJ8z3IdSNNoyq4QF_E4W8Ozs76V9727DiCfEhZQ5ieJYFpMbwLY1R";
        String clientSecret = "B36A118D380A3FD4120CCB2FADFBE8913A09DDA01DFD4292E08C3D817A7C6AC7";
        //String endpoint = "https://lloydsbank--devpoc1.sandbox.my.salesforce.com";

        // Construct the request body
        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("grant_type", "client_credentials");
        requestBody.put("client_id", clientId);
        requestBody.put("client_secret", clientSecret);

        // Prepare the request URL and headers
        String url = endpoint + "/services/oauth2/token";
        String body = requestBody.entrySet()
                .stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .reduce((param1, param2) -> param1 + "&" + param2)
                .orElse("");

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        // Send the request and process the response
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                String jsonResponse = response.body();
                System.out.println("API Response: " + jsonResponse);

                // Parse the JSON response
                String accessToken = jsonResponse.split("\"access_token\":\"")[1].split("\"")[0];
                System.out.println("Access Token: " + accessToken);
                return accessToken;
            } else {
                System.out.println("Error fetching access token. Response Code: " + response.statusCode());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
