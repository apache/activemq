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

package org.apache.activemq.transport.http;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.transport.http.marshallers.TextWireFormatMarshallers;
import org.apache.activemq.transport.xstream.XStreamWireFormat;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.params.BasicHttpParams;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

public class HttpClientTransportTest {

    @Rule
    public final MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private HttpClient sendHttpClient;

    @Mock
    private HttpClient receiveHttpClient;

    @Test
    public void testPreservesAsymmetricalMarshalling() throws Exception {
        final AtomicReference<Object> unmarshalledCommand = new AtomicReference<>();

        final HttpClientTransport httpClientTransport = new HttpClientTransport(TextWireFormatMarshallers.newTransportMarshaller(new XStreamWireFormat()), URI.create("http://localhost")) {
            @Override
            public HttpClient getSendHttpClient() {
                return sendHttpClient;
            }

            @Override
            public HttpClient getReceiveHttpClient() {
                return receiveHttpClient;
            }

            @Override
            public void doConsume(final Object command) {
                unmarshalledCommand.set(command);
                try {
                    stop();
                } catch (Exception e) {
                }
            }
        };

        final AtomicReference<String> marshalledCommand = new AtomicReference<>();

        {
            when(sendHttpClient.getParams()).thenReturn(new BasicHttpParams());
            when(sendHttpClient.execute(Mockito.<HttpUriRequest>any())).thenAnswer(new Answer<HttpResponse>() {
                @Override
                public HttpResponse answer(final InvocationOnMock invocation) throws Throwable {
                    final HttpPost method = invocation.getArgumentAt(0, HttpPost.class);
                    final String entityBody = IOUtils.toString(method.getEntity().getContent());
                    marshalledCommand.set(entityBody);
                    return newHttpOkResponse();
                }
            });

            httpClientTransport.oneway(new ConsumerInfo());
            assertThat(marshalledCommand.get(), CoreMatchers.startsWith("<"));
        }

        {
            final BasicHttpResponse httpOkResponse = newHttpOkResponse();
            httpOkResponse.setEntity(new InputStreamEntity(new ByteArrayInputStream(toMarshalledMessage(marshalledCommand))));
            when(receiveHttpClient.execute(Mockito.<HttpUriRequest>any())).thenReturn(httpOkResponse);
            httpClientTransport.run();
            assertThat(unmarshalledCommand.get(), CoreMatchers.instanceOf(ConsumerInfo.class));
        }
    }

    private byte[] toMarshalledMessage(AtomicReference<String> marshalledCommand) throws IOException {
        final byte[] textBytes = marshalledCommand.get().getBytes(StandardCharsets.UTF_8);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dataOutputStream = new DataOutputStream(baos);
        dataOutputStream.writeInt(textBytes.length);
        dataOutputStream.write(textBytes);
        dataOutputStream.flush();
        return baos.toByteArray();
    }

    private BasicHttpResponse newHttpOkResponse() {
        return new BasicHttpResponse(new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), HttpURLConnection.HTTP_OK, "OK"));
    }
}