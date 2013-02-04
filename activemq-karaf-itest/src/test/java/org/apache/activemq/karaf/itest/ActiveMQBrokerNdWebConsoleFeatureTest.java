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
package org.apache.activemq.karaf.itest;

import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;


import static org.junit.Assert.assertEquals;

@RunWith(JUnit4TestRunner.class)
public class ActiveMQBrokerNdWebConsoleFeatureTest extends ActiveMQBrokerFeatureTest {

    static final String WEB_CONSOLE_URL = "http://localhost:8181/activemqweb/";

    @Configuration
    public static Option[] configure() {
        return append(CoreOptions.mavenBundle("commons-codec", "commons-codec").versionAsInProject(),
                append(CoreOptions.mavenBundle("commons-httpclient", "commons-httpclient").versionAsInProject(),
                configureBrokerStart(configure("activemq-broker"))));
    }

    @Override
    protected void produceMessage(String nameAndPayload) throws Exception {
        HttpClient client = new HttpClient();

        System.err.println("attempting publish via web console..");

        // set credentials
        client.getState().setCredentials(
                new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT),
                new UsernamePasswordCredentials(USER, PASSWORD)
         );

        // need to first get the secret
        GetMethod get = new GetMethod(WEB_CONSOLE_URL + "send.jsp");
        get.setDoAuthentication(true);

        // Give console some time to start
        for (int i=0; i<20; i++) {
            TimeUnit.SECONDS.sleep(1);
            try {
                i = client.executeMethod(get);
            } catch (java.net.ConnectException ignored) {}
        }
        assertEquals("get succeeded on " + get, 200, get.getStatusCode());

        String response = get.getResponseBodyAsString();
        final String secretMarker = "<input type=\"hidden\" name=\"secret\" value=\"";
        String secret = response.substring(response.indexOf(secretMarker) + secretMarker.length());
        secret = secret.substring(0, secret.indexOf("\"/>"));

        PostMethod post = new PostMethod(WEB_CONSOLE_URL + "sendMessage.action");
        post.setDoAuthentication(true);
        post.addParameter("secret", secret);

        post.addParameter("JMSText", nameAndPayload);
        post.addParameter("JMSDestination", nameAndPayload);
        post.addParameter("JMSDestinationType", "queue");

        // execute the send
        assertEquals("post succeeded, " + post, 302, client.executeMethod(post));
    }
}
