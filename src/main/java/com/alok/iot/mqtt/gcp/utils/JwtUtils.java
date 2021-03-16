package com.alok.iot.mqtt.gcp.utils;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

public class JwtUtils {

    /** Create a Cloud IoT Core JWT for the given project id, signed with the given RSA key. */
    public static String createJwtRsa(String projectId, String privateKeyFile)
            throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        DateTime now = new DateTime();
        // Create a JWT to authenticate this device. The device will be disconnected after the token
        // expires, and will have to reconnect with a new token. The audience field should always be set
        // to the GCP project id.
        JwtBuilder jwtBuilder =
                Jwts.builder()
                        .setIssuedAt(now.toDate())
                        .setExpiration(now.plusMinutes(20).toDate())
                        .setAudience(projectId);

        byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyFile));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");

        System.out.println("Alok JWT token: " + jwtBuilder.signWith(SignatureAlgorithm.RS256, kf.generatePrivate(spec)).compact());

        return jwtBuilder.signWith(SignatureAlgorithm.RS256, kf.generatePrivate(spec)).compact();
    }

    /** Create a Cloud IoT Core JWT for the given project id, signed with the given ES key. */
    public static String createJwtEs(String projectId, String privateKeyFile)
            throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        DateTime now = new DateTime();
        // Create a JWT to authenticate this device. The device will be disconnected after the token
        // expires, and will have to reconnect with a new token. The audience field should always be set
        // to the GCP project id.
        JwtBuilder jwtBuilder =
                Jwts.builder()
                        .setIssuedAt(now.toDate())
                        .setExpiration(now.plusMinutes(20).toDate())
                        .setAudience(projectId);

        byte[] keyBytes = Files.readAllBytes(Paths.get(privateKeyFile));
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory kf = KeyFactory.getInstance("EC");

        return jwtBuilder.signWith(SignatureAlgorithm.ES256, kf.generatePrivate(spec)).compact();
    }
}
