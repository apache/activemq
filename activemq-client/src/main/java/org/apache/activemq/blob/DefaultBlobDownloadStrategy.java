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
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import jakarta.jms.JMSException;
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
        validateMessageURL(value);

        final URLConnection connection = value.openConnection();
        if (connection instanceof HttpURLConnection && !BlobTransferPolicy.isHttpFollowRedirects()) {
            final HttpURLConnection http = withRedirectSetting((HttpURLConnection) connection);
            final int responseCode = http.getResponseCode();
            if (isRedirectCode(responseCode)) {
                http.disconnect();
                throw new IOException("GET of " + value + " was redirected (" + responseCode
                    + ") and redirects are turned off by " + BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY);
            }
        }
        return connection.getInputStream();
    }

    public void deleteFile(ActiveMQBlobMessage message) throws IOException, JMSException {
        // the stored URL can differ from the upload URL plus message id
        URL url = message.getURL();
        if (url == null) {
            return;
        }
        validateMessageURL(url);

        HttpURLConnection connection = openHttpConnection(url);
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

    /**
     * Checks a received URL against the upload URL and the allowed protocols.
     */
    protected void validateMessageURL(URL messageURL) throws IOException {
        URL uploadURL = new URL(super.transferPolicy.getUploadUrl());

        if (!messageURL.getProtocol().equals(uploadURL.getProtocol())) {
            throw new IOException("The message URL protocol is incorrect");
        }
        if (!messageURL.getHost().equals(uploadURL.getHost())) {
            throw new IOException("The message URL host is incorrect");
        }
        if (uploadURL.getPort() != 0 && messageURL.getPort() != uploadURL.getPort()) {
            throw new IOException("The message URL port is incorrect");
        }
        // ".." is refused, servers resolve it differently
        final String messagePath = decodedPath(messageURL);
        if (!messagePath.startsWith(decodedPath(uploadURL)) || Arrays.asList(messagePath.split("/")).contains("..")) {
            throw new IOException("The message URL path is incorrect");
        }
        checkProtocolAllowed(messageURL);
    }

    private static String decodedPath(URL url) throws IOException {
        try {
            final String path = url.toURI().getPath();
            return path == null ? "" : path;
        } catch (URISyntaxException e) {
            throw new IOException("The message URL path is incorrect", e);
        }
    }

}
