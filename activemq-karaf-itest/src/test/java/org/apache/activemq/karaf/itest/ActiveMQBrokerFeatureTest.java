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

import static org.junit.Assert.assertEquals;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.Test;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.WrappedUrlProvisionOption;


public class ActiveMQBrokerFeatureTest extends AbstractFeatureTest {
	static final String WEB_CONSOLE_URL = "http://localhost:8181/activemqweb/";

    @Configuration
    public static Option[] configure() {
        return new Option[] //
        {
         configure("connector", "activemq-broker"), //
         // To access web console
         //mavenBundle("commons-codec", "commons-codec").versionAsInProject(),
         mavenBundle("org.apache.httpcomponents", "httpcore-osgi").version("4.4.4"),
         mavenBundle("org.apache.httpcomponents", "httpclient-osgi").version("4.5.2"),
         configureBrokerStart()
        };
    }

    protected String installWrappedBundle(WrappedUrlProvisionOption option) {
        return executeCommand("bundle:install 'wrap:" + option.getURL() + "'");
    }

    @Test(timeout=5 * 60 * 1000)
    public void test() throws Throwable {
        assertBrokerStarted();
        JMSTester jms = new JMSTester();
        jms.produceAndConsume(sessionFactory);
        jms.tempSendReceive();
        jms.close();
    }

    private void produceMessageWebConsole(String nameAndPayload) throws Exception {
    	CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new org.apache.http.auth.AuthScope("httpbin.org", 80),
                new org.apache.http.auth.UsernamePasswordCredentials(KarafShellHelper.USER, KarafShellHelper.PASSWORD));
    	CloseableHttpClient client = HttpClientBuilder.create() //
    			.setDefaultCredentialsProvider(credsProvider)
    			.build();

        System.err.println(executeCommand("activemq:bstat").trim());
        System.err.println("attempting to access web console..");

		withinReason(new Callable<Boolean>() {
			public Boolean call() throws Exception {
				CloseableHttpResponse response = client.execute(new HttpGet(WEB_CONSOLE_URL + "index.jsp"));
                return response.getStatusLine().getStatusCode() != 200;
			}
		});

        System.err.println("attempting publish via web console..");

        // need to first get the secret
        CloseableHttpResponse response = client.execute(new HttpGet(WEB_CONSOLE_URL + "send.jsp"));
        int code = response.getStatusLine().getStatusCode();
        assertEquals("getting send succeeded", 200, code);

        String secret = getSecret(EntityUtils.toString(response.getEntity()));

        URI sendUri = new URIBuilder(WEB_CONSOLE_URL + "sendMessage.action") //
        		.addParameter("secret", secret) //
        		.addParameter("JMSText", nameAndPayload)
        		.addParameter("JMSDestination", nameAndPayload)
        		.addParameter("JMSDestinationType", "queue")
        		.build();
        HttpPost post = new HttpPost(sendUri);
        CloseableHttpResponse sendResponse = client.execute(post);
        assertEquals("post succeeded, " + post, 302, sendResponse.getStatusLine().getStatusCode());
        System.err.println(executeCommand("activemq:bstat").trim());
    }

	private String getSecret(String response) throws IOException {
        final String secretMarker = "<input type=\"hidden\" name=\"secret\" value=\"";
        String secret = response.substring(response.indexOf(secretMarker) + secretMarker.length());
        secret = secret.substring(0, secret.indexOf("\"/>"));
		return secret;
	}
    
    @Test
    public void testSendReceiveWeb() throws Throwable {
        assertBrokerStarted();
        JMSTester jms = new JMSTester();
        final String nameAndPayload = String.valueOf(System.currentTimeMillis());
      	produceMessageWebConsole(nameAndPayload);
        assertEquals("got our message", nameAndPayload, jms.consumeMessage(nameAndPayload));
        jms.tempSendReceive();
        jms.close();
    }

}
