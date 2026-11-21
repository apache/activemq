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
package org.apache.activemq.jaas;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.activemq.util.TextEncryptor;
import org.junit.Test;

public class EncryptionSupportTest {

    // accepts only its own bare token, which is all TextEncryptor requires
    private static final class BareTokenEncryptor implements TextEncryptor {

        @Override
        public String encrypt(String plaintext) {
            return "testv1:" + plaintext;
        }

        @Override
        public String decrypt(String value) {
            if (!value.startsWith("testv1:")) {
                throw new IllegalArgumentException("not a token: " + value);
            }
            return value.substring("testv1:".length());
        }
    }

    @Test
    public void testEncryptorReceivesBareToken() {
        var props = new Properties();
        props.setProperty("user", "ENC(testv1:manager)");
        props.setProperty("other", "plain");

        EncryptionSupport.decrypt(props, new BareTokenEncryptor());

        assertEquals("manager", props.getProperty("user"));
        assertEquals("plain", props.getProperty("other"));
    }
}
