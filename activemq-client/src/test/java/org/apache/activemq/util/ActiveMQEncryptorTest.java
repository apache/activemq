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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.activemq.util.ActiveMQEncryptor.EncryptionException;
import org.junit.Test;

public class ActiveMQEncryptorTest {

    private static final String PASSWORD = "activemq";

    private ActiveMQEncryptor newEncryptor() {
        var encryptor = new ActiveMQEncryptor();
        encryptor.setPassword(PASSWORD);
        return encryptor;
    }

    @Test
    public void testRoundTrip() {
        var encryptor = newEncryptor();
        var plaintexts = new String[] { "password", "sup3rS3cret!", "", "pässwörd 日本語",
                "a".repeat(4096) };
        for (var plaintext : plaintexts) {
            var token = encryptor.encrypt(plaintext);
            assertEquals(plaintext, encryptor.decrypt(token));
            assertEquals(plaintext, encryptor.decrypt(ActiveMQEncryptor.wrapEncryptedValue(token)));
        }
    }

    @Test
    public void testTokenFormat() {
        var token = newEncryptor().encrypt("password");
        var parts = token.split(":", -1);
        assertEquals(5, parts.length);
        assertEquals("v2", parts[0]);
        assertEquals(Integer.toString(ActiveMQEncryptor.DEFAULT_ITERATIONS), parts[1]);
    }

    @Test
    public void testTokensAreSaltedPerValue() {
        var encryptor = newEncryptor();
        assertNotEquals(encryptor.encrypt("password"), encryptor.encrypt("password"));
    }

    @Test
    public void testWrongPasswordFails() {
        var token = newEncryptor().encrypt("password");
        var other = new ActiveMQEncryptor();
        other.setPassword("not-the-password");
        try {
            other.decrypt(token);
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
    }

    @Test
    public void testTamperedCiphertextFails() {
        var encryptor = newEncryptor();
        var token = encryptor.encrypt("password");
        var parts = token.split(":", -1);
        var ct = parts[4].toCharArray();
        ct[0] = ct[0] == 'A' ? 'B' : 'A';
        var tampered = String.join(":", parts[0], parts[1], parts[2], parts[3], new String(ct));
        try {
            encryptor.decrypt(tampered);
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
    }

    @Test
    public void testTamperedIterationCountFails() {
        var encryptor = newEncryptor();
        var token = encryptor.encrypt("password");
        var parts = token.split(":", -1);
        // a different, in-range iteration count breaks both the derived key
        // and the GCM additional authenticated data
        var tampered = String.join(":", parts[0], "200000", parts[2], parts[3], parts[4]);
        try {
            encryptor.decrypt(tampered);
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
    }

    @Test
    public void testIterationBounds() {
        var encryptor = newEncryptor();
        try {
            encryptor.setIterations(ActiveMQEncryptor.MIN_ITERATIONS - 1);
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
        try {
            encryptor.decrypt("v2:5000:AAAAAAAAAAAAAAAAAAAAAA==:AAAAAAAAAAAAAAAA:AAAAAAAAAAAAAAAAAAAAAA==");
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
    }

    @Test
    public void testMalformedTokens() {
        var encryptor = newEncryptor();
        for (var malformed : new String[] { "v2:210000:short", "v2:x:AA==:AA==:AA==", "not base64 at all!" }) {
            try {
                encryptor.decrypt(malformed);
                fail("expected EncryptionException for: " + malformed);
            } catch (EncryptionException expected) {
            }
        }
    }

    @Test
    public void testMissingPassword() {
        var encryptor = new ActiveMQEncryptor();
        encryptor.setPasswordEnvName("ACTIVEMQ_ENCRYPTION_PASSWORD_MISSING_FOR_TEST");
        try {
            encryptor.encrypt("password");
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
            assertTrue(expected.getMessage().contains("ACTIVEMQ_ENCRYPTION_PASSWORD_MISSING_FOR_TEST"));
        }
    }

    // Legacy values below were generated with jasypt 1.9.3, the library and
    // version used by previous ActiveMQ releases.

    @Test
    public void testLegacyDefaultAlgorithm() {
        var encryptor = newEncryptor();
        assertEquals("password", encryptor.decrypt("5rwbgw8juwQVThwB3olO8/FBCuc6noaa"));
        assertEquals("system", encryptor.decrypt("n1A95YJfladVoGM4MaxnqA=="));
        assertEquals("manager", encryptor.decrypt("4Pp68aOwkNEyvCcPBDCJAw=="));
        assertEquals("sup3rS3cret!", encryptor.decrypt("YGM6Gq+tZEYudH9mA4HRBP5BabFjs7sN"));
        assertEquals("", encryptor.decrypt("V7y5E66FO3+hA102OH65YA=="));
    }

    @Test
    public void testLegacyValuesShippedInPreviousReleases() {
        // the ENC() values from conf/credentials-enc.properties of previous
        // releases, encrypted with the documented password "activemq"
        var encryptor = newEncryptor();
        assertEquals("manager", encryptor.decrypt("ENC(mYRkg+4Q4hua1kvpCCI2hg==)"));
        assertEquals("password", encryptor.decrypt("ENC(Cf3Jf3tM+UrSOoaKU50od5CuBa8rxjoL)"));
    }

    @Test
    public void testLegacyAesAlgorithm() {
        var encryptor = newEncryptor();
        encryptor.setLegacyAlgorithm("PBEWITHHMACSHA256ANDAES_256");
        assertEquals("password", encryptor.decrypt("86tgPCI2ijAjlvS1dN1c/MoiX/lp9Ip6zhait9Y4o1aQ9j4kmtMKPYDH02Kf7D7D"));
        assertEquals("system", encryptor.decrypt("7v/MiZcQu3WZL9bWDRyEZpo7cY6abCyjoLOL/CQHNxkiSg0P74VQVYBTnwIJsMs6"));
        assertEquals("manager", encryptor.decrypt("7YWTla670LauAz83dtD3lS6WqHDEr5/L1SAgVaZjNiSDnPTAZ85TtfuhjY2SKHf5"));
        assertEquals("sup3rS3cret!", encryptor.decrypt("PmbWpYebDKcAG3nzHYtjqHnFWb7fl8jw7+pBnzQL43Zzbz9/W5pXMJWNddSXeGoC"));
        assertEquals("", encryptor.decrypt("Y7Cwze291JDTZsH20gI+XEE4knqWW9AkFt6hyOp6pYI5J7D3Z5YuTJIft59FqNJG"));
    }

    @Test
    public void testLegacyTripleDesAlgorithm() {
        var encryptor = newEncryptor();
        encryptor.setLegacyAlgorithm("PBEWithMD5AndTripleDES");
        assertEquals("password", encryptor.decrypt("Km/G+J93dEvL1RnzZ3Rj9jyvoiU1W+P+"));
    }

    @Test
    public void testLegacyAesSha512Algorithm() {
        var encryptor = new ActiveMQEncryptor();
        encryptor.setPassword("s0me/Other=Pass");
        encryptor.setLegacyAlgorithm("PBEWITHHMACSHA512ANDAES_256");
        assertEquals("password", encryptor.decrypt("qOggZteCbOjByJLX9zLYCsAGHR1r9Cynoh3ZxbVQN++gpSgK79X9J7gfflqeEEal"));
    }

    @Test
    public void testLegacyWrongPassword() {
        var encryptor = new ActiveMQEncryptor();
        encryptor.setPassword("not-the-password");
        try {
            var result = encryptor.decrypt("5rwbgw8juwQVThwB3olO8/FBCuc6noaa");
            // a wrong PBE password fails PKCS5 padding validation except when
            // the garbage padding is coincidentally valid; the plaintext then differs
            assertNotEquals("password", result);
        } catch (EncryptionException expected) {
        }
    }

    @Test
    public void testTextAadRoundTripAndMismatch() {
        var encryptor = newEncryptor();
        var aad = "ID:producer-1:42".getBytes();
        var token = encryptor.encrypt("payload", aad);
        assertEquals("payload", encryptor.decrypt(token, aad));
        try {
            encryptor.decrypt(token, "ID:other-message".getBytes());
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
        try {
            encryptor.decrypt(token);
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
    }

    @Test
    public void testBinaryRoundTrip() {
        var encryptor = newEncryptor();
        var body = new byte[64 * 1024];
        for (var i = 0; i < body.length; i++) {
            body[i] = (byte) i;
        }
        var frame = encryptor.encrypt(body, null);
        assertEquals(2, frame[0]);
        org.junit.Assert.assertArrayEquals(body, encryptor.decrypt(frame, null));

        var aad = "queue://TEST.Q".getBytes();
        var framed = encryptor.encrypt(body, aad);
        org.junit.Assert.assertArrayEquals(body, encryptor.decrypt(framed, aad));
        try {
            encryptor.decrypt(framed, "queue://OTHER.Q".getBytes());
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
    }

    @Test
    public void testBinaryTamperAndTruncation() {
        var encryptor = newEncryptor();
        var frame = encryptor.encrypt("password".getBytes(), null);
        var tampered = frame.clone();
        tampered[tampered.length - 1] ^= 0x01;
        try {
            encryptor.decrypt(tampered, null);
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
        try {
            encryptor.decrypt(new byte[8], null);
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
        var badVersion = frame.clone();
        badVersion[0] = 9;
        try {
            encryptor.decrypt(badVersion, null);
            fail("expected EncryptionException");
        } catch (EncryptionException expected) {
        }
    }

    @Test
    public void testSaltReuseMode() {
        var encryptor = newEncryptor();
        encryptor.setSaltPerValue(false);
        var first = encryptor.encrypt("one");
        var second = encryptor.encrypt("two");
        // same salt field, distinct IVs, tokens still decrypt independently
        assertEquals(first.split(":", -1)[2], second.split(":", -1)[2]);
        assertNotEquals(first.split(":", -1)[3], second.split(":", -1)[3]);
        assertEquals("one", encryptor.decrypt(first));
        assertEquals("two", encryptor.decrypt(second));

        // a decryptor configured normally reads them fine (token is self-describing)
        assertEquals("one", newEncryptor().decrypt(first));
    }

    @Test
    public void testInterfaces() {
        TextEncryptor text = newEncryptor();
        assertEquals("password", text.decrypt(text.encrypt("password")));
        BytesEncryptor data = newEncryptor();
        org.junit.Assert.assertArrayEquals("password".getBytes(),
                data.decrypt(data.encrypt("password".getBytes(), null), null));
    }

    @Test
    public void testValueWrapping() {
        assertTrue(ActiveMQEncryptor.isEncryptedValue("ENC(abc)"));
        assertTrue(ActiveMQEncryptor.isEncryptedValue("  ENC(abc)  "));
        assertFalse(ActiveMQEncryptor.isEncryptedValue("abc"));
        assertFalse(ActiveMQEncryptor.isEncryptedValue(null));
        assertFalse(ActiveMQEncryptor.isEncryptedValue("ENC(abc"));
        assertEquals("abc", ActiveMQEncryptor.unwrapEncryptedValue("ENC(abc)"));
        assertEquals("ENC(abc)", ActiveMQEncryptor.wrapEncryptedValue("abc"));
    }
}
