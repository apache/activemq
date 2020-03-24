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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.jms.JMSException;
import org.apache.activemq.command.ActiveMQBlobMessage;

/**
 * A default implementation of {@link BlobDownloadStrategy} which uses the URL
 * class to download files or streams from a remote URL
 */
public class DefaultBlobDownloadStrategy extends DefaultStrategy implements BlobDownloadStrategy {

    public DefaultBlobDownloadStrategy(BlobTransferPolicy transferPolicy) {
        super(transferPolicy);
    }

    public InputStream getInputStream(ActiveMQBlobMessage message) throws IOException, JMSException {
        URL value = message.getURL();
        if (value == null) {
            return null;
        }

        // Do some checks on the received URL against the transfer policy
        URL uploadURL = new URL(super.transferPolicy.getUploadUrl());
        String protocol = message.getURL().getProtocol();
        if (!protocol.equals(uploadURL.getProtocol())) {
            throw new IOException("The message URL protocol is incorrect");
        }

        String host = message.getURL().getHost();
        if (!host.equals(uploadURL.getHost())) {
            throw new IOException("The message URL host is incorrect");
        }

        int port = message.getURL().getPort();
        if (uploadURL.getPort() != 0 && port != uploadURL.getPort()) {
            throw new IOException("The message URL port is incorrect");
        }

        return value.openStream();
    }

    public void deleteFile(ActiveMQBlobMessage message) throws IOException, JMSException {
        URL url = createMessageURL(message);

        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("DELETE");
        try {
            connection.connect();
        } catch (IOException e) {
            throw new IOException("DELETE failed on: " + url, e);
        } finally {
            connection.disconnect();
        }
        if (!isSuccessfulCode(connection.getResponseCode())) {
            throw new IOException("DELETE was not successful: " + connection.getResponseCode() + " "
                                  + connection.getResponseMessage());
        }
    }

}
