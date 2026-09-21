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
package org.apache.activemq.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encrypts and decrypts values such as configuration properties, message
 * properties and message bodies. Values in configuration files are
 * wrapped in an {@code ENC(...)} marker.
 *
 * <p>Encryption always produces the current format: AES-256-GCM with a key
 * derived via PBKDF2WithHmacSHA512 from the configured password. The text
 * token is self-describing, {@code v2:<iterations>:<salt>:<iv>:<ciphertext>}
 * with base64 fields, so the key-derivation cost can change without breaking
 * existing values. The binary frame produced by
 * {@link #encrypt(byte[], byte[])} carries the same fields:
 * {@code [version 0x02][iterations][salt][iv][ciphertext]}.
 * Format version 3 is reserved for values encrypted with an externally
 * managed key referenced by id instead of a password-derived key.
 *
 * <p>Decryption of text values also accepts values created by the jasypt
 * library used by previous ActiveMQ releases. A legacy value is a plain
 * base64 payload laid out as {@code salt || [iv ||] ciphertext} with a fixed
 * 1000 key-derivation iterations: the salt length equals the cipher block
 * size (8 bytes for the default PBEWithMD5AndDES, 16 for AES algorithms) and
 * a 16-byte IV is present only for AES algorithms. Legacy values decrypt
 * transparently with a warning; re-encrypt them with the
 * {@code activemq encrypt} command.
 *
 * <p>The encryption password is taken from the {@link #setPassword password}
 * property when set, otherwise from the environment variable named by
 * {@link #setPasswordEnvName passwordEnvName} (default
 * {@value #DEFAULT_PASSWORD_ENV_NAME}).
 *
 * <p>Decryption caches derived keys per salt, so decrypting many values
 * encrypted under the same salt runs PBKDF2 once. Encryption generates a
 * salt and derives a key per value by default. For high-rate use such as
 * message properties or bodies, set {@link #setSaltPerValue
 * saltPerValue=false}: the instance reuses one salt and derived key,
 * generates a random IV per value, and replaces the salt after 2^30 values.
 *
 * <p>Instances are safe for concurrent use once configured.
 */
public class ActiveMQEncryptor implements TextEncryptor, BytesEncryptor {

    public static final String DEFAULT_PASSWORD_ENV_NAME = "ACTIVEMQ_ENCRYPTION_PASSWORD";
    public static final String DEFAULT_LEGACY_ALGORITHM = "PBEWithMD5AndDES";
    public static final int DEFAULT_ITERATIONS = 210_000;

    public static final int MIN_ITERATIONS = 10_000;
    public static final int MAX_ITERATIONS = 10_000_000;

    private static final Logger LOG = LoggerFactory.getLogger(ActiveMQEncryptor.class);

    private static final String ENC_PREFIX = "ENC(";
    private static final String ENC_SUFFIX = ")";

    private static final String TOKEN_VERSION = "v2";
    private static final byte FRAME_VERSION = 2;
    private static final String KEY_DERIVATION_ALGORITHM = "PBKDF2WithHmacSHA512";
    private static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";
    private static final String KEY_ALGORITHM = "AES";
    private static final int KEY_LENGTH_BITS = 256;
    private static final int SALT_LENGTH_BYTES = 16;
    private static final int GCM_IV_LENGTH_BYTES = 12;
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final int FRAME_HEADER_LENGTH = 1 + 4 + SALT_LENGTH_BYTES + GCM_IV_LENGTH_BYTES;

    private static final int KEY_CACHE_SIZE = 16;
    // NIST SP 800-38D caps random-IV GCM at 2^32 invocations per key;
    // replace the salt earlier
    private static final long MAX_USES_PER_SALT = 1L << 30;

    // jasypt used a fixed iteration count and derived the salt length from
    // the cipher block size; AES algorithms additionally carry a 16-byte IV
    private static final int LEGACY_ITERATIONS = 1000;
    private static final int LEGACY_SALT_LENGTH_BYTES = 8;
    private static final int LEGACY_AES_SALT_LENGTH_BYTES = 16;
    private static final int LEGACY_AES_IV_LENGTH_BYTES = 16;

    private static final SecureRandom RANDOM = new SecureRandom();

    private String password;
    private String passwordEnvName = DEFAULT_PASSWORD_ENV_NAME;
    private String legacyAlgorithm = DEFAULT_LEGACY_ALGORITHM;
    private int iterations = DEFAULT_ITERATIONS;
    private boolean saltPerValue = true;

    private final Map<String, SecretKey> keyCache =
            Collections.synchronizedMap(new LinkedHashMap<>(KEY_CACHE_SIZE, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, SecretKey> eldest) {
                    return size() > KEY_CACHE_SIZE;
                }
            });

    private final Object saltLock = new Object();
    private byte[] reusedSalt;
    private long reusedSaltUses;

    /**
     * Encrypts the given text with the current format. The returned token is
     * not wrapped; store it in configuration as {@code ENC(<token>)}.
     */
    @Override
    public String encrypt(String plaintext) {
        return encrypt(plaintext, (byte[]) null);
    }

    /**
     * Encrypts the given text, binding the optional additional authenticated
     * data into the authentication tag. The same AAD must be supplied to
     * {@link #decrypt(String, byte[])}.
     */
    public String encrypt(String plaintext, byte[] aad) {
        if (plaintext == null) {
            throw new EncryptionException("no input to encrypt");
        }
        var salt = nextSalt();
        var iv = new byte[GCM_IV_LENGTH_BYTES];
        RANDOM.nextBytes(iv);
        var iterationsField = Integer.toString(iterations);
        var ciphertext = runGcm(Cipher.ENCRYPT_MODE, salt, iterations, iv,
                textHeaderAad(iterationsField), aad, plaintext.getBytes(StandardCharsets.UTF_8));

        var encoder = Base64.getEncoder();
        return TOKEN_VERSION + ':' + iterationsField
                + ':' + encoder.encodeToString(salt)
                + ':' + encoder.encodeToString(iv)
                + ':' + encoder.encodeToString(ciphertext);
    }

    /**
     * Decrypts a value produced by {@link #encrypt(String)} or by the
     * jasypt-based tooling of previous releases. Accepts the bare token or
     * an {@code ENC(...)} wrapped value.
     */
    @Override
    public String decrypt(String input) {
        return decrypt(input, (byte[]) null);
    }

    /**
     * Decrypts a value produced by {@link #encrypt(String, byte[])} with the
     * same additional authenticated data.
     */
    public String decrypt(String input, byte[] aad) {
        if (input == null) {
            throw new EncryptionException("no input to decrypt");
        }
        var token = input.trim();
        if (isEncryptedValue(token)) {
            token = unwrapEncryptedValue(token);
        }
        if (token.startsWith(TOKEN_VERSION + ':')) {
            return decryptToken(token, aad);
        }
        if (aad != null) {
            throw new EncryptionException("legacy values do not support additional authenticated data");
        }
        return decryptLegacy(token);
    }

    /**
     * Encrypts the given data with the current format as a compact binary
     * frame: {@code [version 0x02][iterations][salt][iv][ciphertext]}.
     */
    @Override
    public byte[] encrypt(byte[] plaintext, byte[] aad) {
        if (plaintext == null) {
            throw new EncryptionException("no input to encrypt");
        }
        var salt = nextSalt();
        var iv = new byte[GCM_IV_LENGTH_BYTES];
        RANDOM.nextBytes(iv);
        var header = frameHeader(iterations, salt, iv);
        var ciphertext = runGcm(Cipher.ENCRYPT_MODE, salt, iterations, iv, header, aad, plaintext);

        var frame = Arrays.copyOf(header, header.length + ciphertext.length);
        System.arraycopy(ciphertext, 0, frame, header.length, ciphertext.length);
        return frame;
    }

    /**
     * Decrypts a frame produced by {@link #encrypt(byte[], byte[])} with the
     * same additional authenticated data.
     */
    @Override
    public byte[] decrypt(byte[] payload, byte[] aad) {
        if (payload == null) {
            throw new EncryptionException("no input to decrypt");
        }
        if (payload.length < FRAME_HEADER_LENGTH + GCM_TAG_LENGTH_BITS / 8) {
            throw new EncryptionException("encrypted payload too short");
        }
        if (payload[0] != FRAME_VERSION) {
            throw new EncryptionException("unsupported encrypted payload version " + payload[0]);
        }
        var buffer = ByteBuffer.wrap(payload);
        buffer.get();
        var frameIterations = buffer.getInt();
        if (frameIterations < MIN_ITERATIONS || frameIterations > MAX_ITERATIONS) {
            throw new EncryptionException("iteration count " + frameIterations
                    + " outside supported range [" + MIN_ITERATIONS + ", " + MAX_ITERATIONS + "]");
        }
        var salt = new byte[SALT_LENGTH_BYTES];
        buffer.get(salt);
        var iv = new byte[GCM_IV_LENGTH_BYTES];
        buffer.get(iv);
        var ciphertext = new byte[buffer.remaining()];
        buffer.get(ciphertext);

        var header = frameHeader(frameIterations, salt, iv);
        return runGcm(Cipher.DECRYPT_MODE, salt, frameIterations, iv, header, aad, ciphertext);
    }

    private String decryptToken(String token, byte[] aad) {
        var parts = token.split(":", -1);
        if (parts.length != 5) {
            throw new EncryptionException("malformed encrypted value: expected "
                    + TOKEN_VERSION + ":<iterations>:<salt>:<iv>:<ciphertext>");
        }
        int tokenIterations;
        try {
            tokenIterations = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new EncryptionException("malformed encrypted value: invalid iteration count", e);
        }
        if (tokenIterations < MIN_ITERATIONS || tokenIterations > MAX_ITERATIONS) {
            throw new EncryptionException("iteration count " + tokenIterations
                    + " outside supported range [" + MIN_ITERATIONS + ", " + MAX_ITERATIONS + "]");
        }
        var salt = decodeBase64(parts[2]);
        var iv = decodeBase64(parts[3]);
        var ciphertext = decodeBase64(parts[4]);
        var plaintext = runGcm(Cipher.DECRYPT_MODE, salt, tokenIterations, iv,
                textHeaderAad(parts[1]), aad, ciphertext);
        return new String(plaintext, StandardCharsets.UTF_8);
    }

    private byte[] runGcm(int mode, byte[] salt, int keyIterations, byte[] iv,
            byte[] headerAad, byte[] extraAad, byte[] input) {
        try {
            var key = deriveKeyCached(salt, keyIterations);
            var cipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
            cipher.init(mode, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
            cipher.updateAAD(headerAad);
            if (extraAad != null) {
                cipher.updateAAD(extraAad);
            }
            return cipher.doFinal(input);
        } catch (GeneralSecurityException e) {
            if (mode == Cipher.ENCRYPT_MODE) {
                throw new EncryptionException("unable to encrypt value", e);
            }
            throw new EncryptionException(
                    "unable to decrypt value: wrong password, wrong authenticated data, or corrupted value", e);
        }
    }

    private String decryptLegacy(String token) {
        var algorithm = legacyAlgorithm != null ? legacyAlgorithm : DEFAULT_LEGACY_ALGORITHM;
        var payload = decodeBase64(token);
        var aes = algorithm.toUpperCase(Locale.ROOT).contains("AES");
        var saltLength = aes ? LEGACY_AES_SALT_LENGTH_BYTES : LEGACY_SALT_LENGTH_BYTES;
        var ivLength = aes ? LEGACY_AES_IV_LENGTH_BYTES : 0;
        if (payload.length <= saltLength + ivLength) {
            throw new EncryptionException("encrypted value too short for legacy " + algorithm + " format");
        }
        var salt = Arrays.copyOfRange(payload, 0, saltLength);
        var ciphertext = Arrays.copyOfRange(payload, saltLength + ivLength, payload.length);

        var passwordChars = resolvePassword();
        try {
            var keySpec = new PBEKeySpec(passwordChars);
            SecretKey key;
            try {
                key = SecretKeyFactory.getInstance(algorithm).generateSecret(keySpec);
            } finally {
                keySpec.clearPassword();
            }
            PBEParameterSpec parameterSpec;
            if (aes) {
                var ivSpec = new IvParameterSpec(payload, saltLength, ivLength);
                parameterSpec = new PBEParameterSpec(salt, LEGACY_ITERATIONS, ivSpec);
            } else {
                parameterSpec = new PBEParameterSpec(salt, LEGACY_ITERATIONS);
            }
            var cipher = Cipher.getInstance(algorithm);
            cipher.init(Cipher.DECRYPT_MODE, key, parameterSpec);
            var plaintext = new String(cipher.doFinal(ciphertext), StandardCharsets.UTF_8);
            LOG.warn("Decrypted a value in the legacy jasypt {} format. Legacy values use weak cryptography;"
                    + " re-encrypt with the 'activemq encrypt' command to upgrade to AES-256-GCM.", algorithm);
            return plaintext;
        } catch (GeneralSecurityException e) {
            throw new EncryptionException("unable to decrypt legacy value: wrong password, corrupted value,"
                    + " or wrong legacy algorithm (" + algorithm + ")", e);
        } finally {
            Arrays.fill(passwordChars, '\0');
        }
    }

    private SecretKey deriveKeyCached(byte[] salt, int keyIterations) throws GeneralSecurityException {
        var cacheKey = keyIterations + ":" + Base64.getEncoder().encodeToString(salt);
        var cached = keyCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        var passwordChars = resolvePassword();
        try {
            var keySpec = new PBEKeySpec(passwordChars, salt, keyIterations, KEY_LENGTH_BITS);
            try {
                var derived = SecretKeyFactory.getInstance(KEY_DERIVATION_ALGORITHM).generateSecret(keySpec);
                var key = new SecretKeySpec(derived.getEncoded(), KEY_ALGORITHM);
                keyCache.put(cacheKey, key);
                return key;
            } finally {
                keySpec.clearPassword();
            }
        } finally {
            Arrays.fill(passwordChars, '\0');
        }
    }

    private byte[] nextSalt() {
        if (saltPerValue) {
            var salt = new byte[SALT_LENGTH_BYTES];
            RANDOM.nextBytes(salt);
            return salt;
        }
        synchronized (saltLock) {
            if (reusedSalt == null || reusedSaltUses >= MAX_USES_PER_SALT) {
                reusedSalt = new byte[SALT_LENGTH_BYTES];
                RANDOM.nextBytes(reusedSalt);
                reusedSaltUses = 0;
            }
            reusedSaltUses++;
            return reusedSalt;
        }
    }

    private static byte[] textHeaderAad(String iterationsField) {
        return (TOKEN_VERSION + ':' + iterationsField).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] frameHeader(int keyIterations, byte[] salt, byte[] iv) {
        var header = ByteBuffer.allocate(FRAME_HEADER_LENGTH);
        header.put(FRAME_VERSION);
        header.putInt(keyIterations);
        header.put(salt);
        header.put(iv);
        return header.array();
    }

    private static byte[] decodeBase64(String value) {
        try {
            return Base64.getDecoder().decode(value);
        } catch (IllegalArgumentException e) {
            throw new EncryptionException("encrypted value is not valid base64", e);
        }
    }

    private char[] resolvePassword() {
        var resolved = password;
        if (resolved == null || resolved.isEmpty()) {
            resolved = System.getenv(passwordEnvName);
        }
        if (resolved == null || resolved.isEmpty()) {
            throw new EncryptionException("encryption password not set: set the " + passwordEnvName
                    + " environment variable or configure the password property");
        }
        return resolved.toCharArray();
    }

    /**
     * @return true if the value is wrapped in the {@code ENC(...)} marker
     */
    public static boolean isEncryptedValue(String value) {
        if (value == null) {
            return false;
        }
        var trimmed = value.trim();
        return trimmed.startsWith(ENC_PREFIX) && trimmed.endsWith(ENC_SUFFIX);
    }

    /**
     * @return the token inside an {@code ENC(...)} wrapped value
     */
    public static String unwrapEncryptedValue(String value) {
        var trimmed = value.trim();
        return trimmed.substring(ENC_PREFIX.length(), trimmed.length() - ENC_SUFFIX.length());
    }

    /**
     * @return the token wrapped in the {@code ENC(...)} marker used in
     *         configuration files
     */
    public static String wrapEncryptedValue(String token) {
        return ENC_PREFIX + token + ENC_SUFFIX;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
        keyCache.clear();
    }

    public String getPasswordEnvName() {
        return passwordEnvName;
    }

    public void setPasswordEnvName(String passwordEnvName) {
        this.passwordEnvName = passwordEnvName;
        keyCache.clear();
    }

    /**
     * JCE PBE algorithm used to decrypt values created by the jasypt-based
     * tooling of previous releases, for example {@code PBEWithMD5AndDES}
     * (the previous default) or {@code PBEWITHHMACSHA256ANDAES_256}. Only
     * used for decryption of legacy values.
     */
    public String getLegacyAlgorithm() {
        return legacyAlgorithm;
    }

    public void setLegacyAlgorithm(String legacyAlgorithm) {
        this.legacyAlgorithm = legacyAlgorithm;
    }

    /**
     * PBKDF2 iteration count used when encrypting new values.
     */
    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        if (iterations < MIN_ITERATIONS || iterations > MAX_ITERATIONS) {
            throw new IllegalArgumentException("iterations must be in range ["
                    + MIN_ITERATIONS + ", " + MAX_ITERATIONS + "]");
        }
        this.iterations = iterations;
    }

    /**
     * When true (the default) every encrypted value gets its own random salt
     * and key derivation, so values are independent of each other. Set to
     * false for high-rate use such as message properties or bodies: the
     * instance then reuses one salt so the derived key is computed once,
     * generates a random IV per value, and replaces the salt after
     * {@code 2^30} values.
     */
    public boolean isSaltPerValue() {
        return saltPerValue;
    }

    public void setSaltPerValue(boolean saltPerValue) {
        this.saltPerValue = saltPerValue;
    }

    /**
     * Thrown when a value cannot be encrypted or decrypted.
     */
    public static class EncryptionException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public EncryptionException(String message) {
            super(message);
        }

        public EncryptionException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
