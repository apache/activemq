/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.security.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.shaded.json.JSONObject;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.io.InputStream;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.List;

/**
 * Utilities for generating a JWT for testing
 */
public class TokenUtils {

    private TokenUtils() {}

    /**
     * Read a PEM encoded private key from the classpath.
     *
     * @param pemResName key file resource name
     * @return the PrivateKey
     * @throws Exception on decode failure
     */
    public static PrivateKey readPrivateKey(String pemResName) throws Exception {
        InputStream contentIS = TokenUtils.class.getResourceAsStream(pemResName);
        byte[] tmp = new byte[4096];
        int length = contentIS.read(tmp);
        return decodePrivateKey(new String(tmp, 0, length));
    }

    /**
     * Read a PEM encoded public key from the classpath.
     *
     * @param pemResName key file resource name
     * @return the PublicKey
     * @throws Exception on decode failure
     */
    public static PublicKey readPublicKey(String pemResName) throws Exception {
        InputStream contentIS = TokenUtils.class.getResourceAsStream(pemResName);
        byte[] tmp = new byte[4096];
        int length = contentIS.read(tmp);
        return decodePublicKey(new String(tmp, 0, length));
    }

    /**
     * Generate a new RSA keypair.
     *
     * @param keySize the size of the key
     * @return the KeyPair
     * @throws NoSuchAlgorithmException on failure to load RSA key generator
     */
    public static KeyPair generateKeyPair(int keySize) throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(keySize);
        return keyPairGenerator.genKeyPair();
    }

    /**
     * Decode a PEM encoded private key string to a RSA PrivateKey.
     *
     * @param pemEncoded PEM string for private key
     * @return the PrivateKey
     * @throws Exception on decode failure
     */
    public static PrivateKey decodePrivateKey(String pemEncoded) throws Exception {
        pemEncoded = removeBeginEnd(pemEncoded);
        byte[] pkcs8EncodedBytes = Base64.getDecoder().decode(pemEncoded);
        // extract the private key
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(keySpec);
    }

    /**
     * Decode a PEM encoded public key string to a RSA PublicKey
     *
     * @param pemEncoded PEM string for public key
     * @return the PublicKey
     * @throws Exception on decode failure
     */
    public static PublicKey decodePublicKey(String pemEncoded) throws Exception {
        pemEncoded = removeBeginEnd(pemEncoded);
        byte[] encodedBytes = Base64.getDecoder().decode(pemEncoded);
        // extract the public key
        X509EncodedKeySpec keySpec = new X509EncodedKeySpec(encodedBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(keySpec);
    }

    private static String removeBeginEnd(String pem) {
        pem = pem.replaceAll("-----BEGIN (.*)-----", "");
        pem = pem.replaceAll("-----END (.*)----", "");
        pem = pem.replaceAll("\r\n", "");
        pem = pem.replaceAll("\n", "");
        return pem.trim();
    }

    public static String token(final String name, final List<String> groups) {
        final JSONObject claims = new JSONObject();

        claims.put(Claims.iss.name(), JwtAuthenticationPlugin.JWT_ISSUER);
        long currentTimeInSecs = System.currentTimeMillis() / 1000;
        claims.put(Claims.iat.name(), currentTimeInSecs);
        claims.put(Claims.auth_time.name(), currentTimeInSecs);
        claims.put(Claims.exp.name(), currentTimeInSecs + 300);
        claims.put(Claims.jti.name(), "a-123");
        claims.put(Claims.sub.name(), "24400320");
        claims.put(Claims.preferred_username.name(), name);
        claims.put(Claims.aud.name(), "s6BhdRkqt3");
        claims.put(Claims.groups.name(), groups);

        try {
            final PrivateKey pk = readPrivateKey(JwtAuthenticationPlugin.JWT_SIGNING_KEY_LOCATION);
            final JWSHeader header = new JWSHeader.Builder(JwtAuthenticationPlugin.JWT_SIGNING_ALGO)
                    .keyID(JwtAuthenticationPlugin.JWT_SIGNING_KEY_LOCATION)
                    .type(JOSEObjectType.JWT)
                    .build();

            final JWTClaimsSet claimsSet = JWTClaimsSet.parse(claims);
            final SignedJWT jwt = new SignedJWT(header, claimsSet);
            jwt.sign(new RSASSASigner(pk));
            return jwt.serialize();

        } catch (final Exception e) {
            throw new RuntimeException("Could not sign JWT");
        }
    }

}
