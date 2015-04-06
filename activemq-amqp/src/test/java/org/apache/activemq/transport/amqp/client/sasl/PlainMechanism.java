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
package org.apache.activemq.transport.amqp.client.sasl;

/**
 * Implements the SASL PLAIN authentication Mechanism.
 *
 * User name and Password values are sent without being encrypted.
 */
public class PlainMechanism extends AbstractMechanism {

    @Override
    public int getPriority() {
        return PRIORITY.MEDIUM.getValue();
    }

    @Override
    public String getName() {
        return "PLAIN";
    }

    @Override
    public byte[] getInitialResponse() {

        String username = getUsername();
        String password = getPassword();

        if (username == null) {
            username = "";
        }

        if (password == null) {
            password = "";
        }

        byte[] usernameBytes = username.getBytes();
        byte[] passwordBytes = password.getBytes();
        byte[] data = new byte[usernameBytes.length + passwordBytes.length + 2];
        System.arraycopy(usernameBytes, 0, data, 1, usernameBytes.length);
        System.arraycopy(passwordBytes, 0, data, 2 + usernameBytes.length, passwordBytes.length);
        return data;
    }

    @Override
    public byte[] getChallengeResponse(byte[] challenge) {
        return EMPTY;
    }
}
