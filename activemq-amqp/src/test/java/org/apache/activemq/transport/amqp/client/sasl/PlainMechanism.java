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

    public static final String MECH_NAME = "PLAIN";

    @Override
    public int getPriority() {
        return PRIORITY.MEDIUM.getValue();
    }

    @Override
    public String getName() {
        return MECH_NAME;
    }

    @Override
    public byte[] getInitialResponse() {

        String authzid = getAuthzid();
        String username = getUsername();
        String password = getPassword();

        if (authzid == null) {
            authzid = "";
        }

        if (username == null) {
            username = "";
        }

        if (password == null) {
            password = "";
        }

        byte[] authzidBytes = authzid.getBytes();
        byte[] usernameBytes = username.getBytes();
        byte[] passwordBytes = password.getBytes();
        byte[] data = new byte[authzidBytes.length + 1 + usernameBytes.length + 1 + passwordBytes.length];
        System.arraycopy(authzidBytes, 0, data, 0, authzidBytes.length);
        System.arraycopy(usernameBytes, 0, data, 1 + authzidBytes.length, usernameBytes.length);
        System.arraycopy(passwordBytes, 0, data, 2 + authzidBytes.length + usernameBytes.length, passwordBytes.length);
        return data;
    }

    @Override
    public byte[] getChallengeResponse(byte[] challenge) {
        return EMPTY;
    }

    @Override
    public boolean isApplicable(String username, String password) {
        return username != null && username.length() > 0 && password != null && password.length() > 0;
    }
}
