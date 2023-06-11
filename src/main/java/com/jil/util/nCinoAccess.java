package com.jil.util;

import com.jil.config.Constants;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
public class nCinoAccess implements Constants {

    public static Supplier<Map<String, String>> getAccessTokenSupplier() {
        return () -> {
            try {
                Map<String, String> tokenResp = callTokeEndpoint(generateJwt());
                log.debug("tokenResp", tokenResp);
                return tokenResp;
            } catch (Exception e) {
                log.error("Error performing access", e);
            }
            return null;
        };
    }

    private static String generateJwt() throws Exception {
        byte[] privateKeyBytes = Files.readAllBytes(Paths.get(PRIVATE_KEY_PATH));
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(privateKeyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PrivateKey privateKey = keyFactory.generatePrivate(keySpec);

        JwtBuilder jwtBuilder = Jwts.builder()
                .setIssuer(CLIENT_ID)
                .setSubject(USERNAME)
                .setAudience(TOKEN_ENDPOINT)
                .setExpiration(Date.from(Instant.now().plusSeconds(1200)));
        String jwt = jwtBuilder
                .signWith(SignatureAlgorithm.RS256, privateKey)
                .compact();
        log.debug("jwt: {}", jwt);
        return jwt;
    }

    private static Map<String, String> callTokeEndpoint(String jwt) {
        WebClient client = WebClient.builder().build();

        String requestBody = "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=" + jwt;

        Mono<String> responseMono = client.post()
                .uri(TOKEN_ENDPOINT)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .body(BodyInserters.fromValue(requestBody))
                .retrieve()
                .bodyToMono(String.class);

        String response = responseMono.block();
        log.debug("response: {}", response);

        Map<String, String> tokenResp = new HashMap<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode responseJson = mapper.readTree(response);
//            {
//                "access_token":"00D3H0000000gL",
//                    "scope":"user_registration_api id api full",
//                    "instance_url":"https://lloydsbank--devpoc1.sandbox.my.salesforce.com",
//                    "id":"https://test.salesforce.com/id/00D3H0000000gLyUAI/0053H000004IveaQAC",
//                    "token_type":"Bearer"
//            }
            tokenResp.put("access_token", responseJson.get("access_token").asText());
            tokenResp.put("instance_url", responseJson.get("instance_url").asText());
            tokenResp.put("id", responseJson.get("id").asText());
            tokenResp.put("token_type", responseJson.get("token_type").asText());

        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON response", e);
        }

        return tokenResp;
    }

}
