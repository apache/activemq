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

/**
 * Encrypts and decrypts binary data, such as message bodies. The payload is
 * a compact self-describing binary frame rather than a base64 text token.
 *
 * <p>The optional additional authenticated data (AAD) is not stored in the
 * payload but is bound into the authentication tag: decryption fails unless
 * the caller supplies the same bytes. Use it to tie a ciphertext to its
 * context, for example a message id or destination name, so an encrypted
 * value cannot be replayed in a different context.
 *
 * @see ActiveMQEncryptor
 */
public interface BytesEncryptor {

    /**
     * Encrypts the given data.
     *
     * @param plaintext the data to encrypt
     * @param aad additional authenticated data to bind into the
     *            authentication tag, or null for none
     */
    byte[] encrypt(byte[] plaintext, byte[] aad);

    /**
     * Decrypts a payload produced by {@link #encrypt(byte[], byte[])}.
     *
     * @param payload the encrypted frame
     * @param aad the additional authenticated data supplied at encryption
     *            time, or null if none was used
     */
    byte[] decrypt(byte[] payload, byte[] aad);
}
