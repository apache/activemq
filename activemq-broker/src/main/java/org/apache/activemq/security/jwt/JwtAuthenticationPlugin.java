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

import com.nimbusds.jose.JWSAlgorithm;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

/**
 * A simple JWT plugin giving ActiveMQ the ability to support JWT tokens to authenticate and authorize users
 *
 * @org.apache.xbean.XBean element="jwtAuthenticationPlugin"
 *                         description="Provides a JWT authentication plugin"
 *
 *
 * This plugin is rather simple and is only meant to be used as a first step. In real world applications, we would need
 * to being able to specify different key format (RSA, DSA, EC, etc), the issuer, the claim to use for groups and maybe
 * some mapping functionalities.
 * The header name is also hard coded for simplicity.
 */
public class JwtAuthenticationPlugin implements BrokerPlugin {

    public static final String JWT_ISSUER = "https://server.example.com";
    public static final Claims JWT_GROUPS_CLAIM = Claims.groups;
    public static final String JWT_SIGNING_KEY_LOCATION = "/privateKey.pem";
    public static final String JWT_VALIDATING_PUBLIC_KEY = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAlivFI8qB4D0y2jy0" +
            "CfEqFyy46R0o7S8TKpsx5xbHKoU1VWg6QkQm+ntyIv1p4kE1sPEQO73+HY8+" +
            "Bzs75XwRTYL1BmR1w8J5hmjVWjc6R2BTBGAYRPFRhor3kpM6ni2SPmNNhurE" +
            "AHw7TaqszP5eUF/F9+KEBWkwVta+PZ37bwqSE4sCb1soZFrVz/UT/LF4tYpu" +
            "VYt3YbqToZ3pZOZ9AX2o1GCG3xwOjkc4x0W7ezbQZdC9iftPxVHR8irOijJR" +
            "RjcPDtA6vPKpzLl6CyYnsIYPd99ltwxTHjr3npfv/3Lw50bAkbT4HeLFxTx4" +
            "flEoZLKO/g0bAoV2uqBhkA9xnQIDAQAB";
    public static final JWSAlgorithm JWT_SIGNING_ALGO = JWSAlgorithm.RS256;
    public static final String JWT_HEADER = "Authorization";

    public JwtAuthenticationPlugin() {
    }

    public Broker installPlugin(final Broker next) {

        // the public key is hard coded here for the sake of the example
        return new JwtAuthenticationBroker(next, JWT_ISSUER, JWT_GROUPS_CLAIM, JWT_VALIDATING_PUBLIC_KEY);

    }

}
