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
package org.apache.activemq.blob;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

import jakarta.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;

public class DefaultStrategy {

    protected BlobTransferPolicy transferPolicy;

    public DefaultStrategy(BlobTransferPolicy transferPolicy) {
        this.transferPolicy = transferPolicy;
    }

    protected void checkProtocolAllowed(URL url) throws IOException {
        BlobTransferPolicy.checkProtocolAllowed(url.getProtocol());
    }

    protected HttpURLConnection openHttpConnection(URL url) throws IOException {
        checkProtocolAllowed(url);
        final URLConnection connection = url.openConnection();
        if (!(connection instanceof HttpURLConnection)) {
            throw new IOException("The default blob strategy needs an http or https URL to upload or delete"
                + " but the protocol is '" + url.getProtocol() + "'");
        }
        return withRedirectSetting((HttpURLConnection) connection);
    }

    protected HttpURLConnection withRedirectSetting(HttpURLConnection connection) {
        if (!BlobTransferPolicy.isHttpFollowRedirects()) {
            connection.setInstanceFollowRedirects(false);
        }
        return connection;
    }

    protected boolean isRedirectCode(int responseCode) {
        return responseCode >= 300 && responseCode < 400;
    }

    protected boolean isSuccessfulCode(int responseCode) {
        return responseCode >= 200 && responseCode < 300; // 2xx => successful
    }

    protected URL createMessageURL(ActiveMQBlobMessage message) throws JMSException, MalformedURLException {
        return new URL(transferPolicy.getUploadUrl() + message.getMessageId().toString());
    }
    
}
