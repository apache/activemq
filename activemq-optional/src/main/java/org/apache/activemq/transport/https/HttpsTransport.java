/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.https;

import org.apache.activemq.transport.http.HttpTransport;
import org.apache.activemq.transport.util.TextWireFormat;

import javax.net.ssl.HttpsURLConnection;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;

public class HttpsTransport extends HttpTransport {

    public HttpsTransport(TextWireFormat wireFormat, URI remoteUrl) throws MalformedURLException {
        super(wireFormat, remoteUrl);
    }

    protected synchronized HttpURLConnection createSendConnection() throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) getRemoteURL().openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        configureConnection(conn);
        conn.connect();
        return conn;
    }

    protected synchronized HttpURLConnection createReceiveConnection() throws IOException {
        HttpsURLConnection conn = (HttpsURLConnection) getRemoteURL().openConnection();
        conn.setDoOutput(false);
        conn.setDoInput(true);
        conn.setRequestMethod("GET");
        configureConnection(conn);
        conn.connect();
        return conn;
    }

}
