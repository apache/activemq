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
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.junit.PaxExam;

import static org.junit.Assert.assertEquals;

@RunWith(PaxExam.class)
@Ignore("Can fail sometimes. Old web-console is also @deprecated")
public class ActiveMQBrokerNdWebConsoleFeatureTest extends ActiveMQBrokerFeatureTest {

    static final String WEB_CONSOLE_URL = "http://localhost:8181/activemqweb/";

    @Configuration
    public static Option[] configure() {
        return append(CoreOptions.mavenBundle("commons-codec", "commons-codec").versionAsInProject(),
                append(CoreOptions.mavenBundle("commons-httpclient", "commons-httpclient").versionAsInProject(),
                configure("activemq-broker")));
    }

    @Override
    protected void produceMessage(String nameAndPayload) throws Exception {
        HttpClient client = new HttpClient();
        client.getHttpConnectionManager().getParams().setConnectionTimeout(30000);

        // set credentials
        client.getState().setCredentials(
                new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT),
                new UsernamePasswordCredentials(USER, PASSWORD)
        );

        System.err.println(executeCommand("activemq:bstat").trim());
        System.err.println("attempting to access web console..");

        GetMethod get = new GetMethod(WEB_CONSOLE_URL + "index.jsp");
        get.setDoAuthentication(true);

        // Give console some time to start
        boolean done = false;
        int loop = 0;
        while (!done && loop < 30) {
            loop++;
            try {
                int code = client.executeMethod(get);
                if (code > 399 && code < 500) {
                    // code 4xx we should retry
                    System.err.println("web console not accessible yet - status code " + code);
                    TimeUnit.SECONDS.sleep(1);
                } else {
                    done = true;
                }
            } catch (Exception ignored) {}
        }
        assertEquals("get succeeded on " + get, 200, get.getStatusCode());

        System.err.println("attempting publish via web console..");

        // need to first get the secret
        get = new GetMethod(WEB_CONSOLE_URL + "send.jsp");
        get.setDoAuthentication(true);

        int code = client.executeMethod(get);
        assertEquals("get succeeded on " + get, 200, code);

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

        System.err.println(executeCommand("activemq:bstat").trim());
    }
}
