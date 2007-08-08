/*
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;

/**
 * A default implementation of {@link BlobUploadStrategy} which uses the URL
 * class to upload files or streams to a remote URL
 */
public class DefaultBlobUploadStrategy implements BlobUploadStrategy {
    private BlobTransferPolicy transferPolicy;

    public DefaultBlobUploadStrategy(BlobTransferPolicy transferPolicy) {
        this.transferPolicy = transferPolicy;
    }

    public URL uploadFile(ActiveMQBlobMessage message, File file) throws JMSException, IOException {
        return uploadStream(message, new FileInputStream(file));
    }

    public URL uploadStream(ActiveMQBlobMessage message, InputStream fis) throws JMSException, IOException {
        URL url = createUploadURL(message);

        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);

        // use chunked mode or otherwise URLConnection loads everything into
        // memory
        // (chunked mode not supported before JRE 1.5)
        connection.setChunkedStreamingMode(transferPolicy.getBufferSize());

        OutputStream os = connection.getOutputStream();

        byte[] buf = new byte[transferPolicy.getBufferSize()];
        for (int c = fis.read(buf); c != -1; c = fis.read(buf)) {
            os.write(buf, 0, c);
            os.flush();
        }
        os.close();
        fis.close();

        if (!isSuccessfulCode(connection.getResponseCode())) {
            throw new IOException("PUT was not successful: " + connection.getResponseCode() + " "
                                  + connection.getResponseMessage());
        }

        return url;
    }

    public void deleteFile(ActiveMQBlobMessage message) throws IOException, JMSException {
        URL url = createUploadURL(message);

        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("DELETE");
        connection.connect();
        connection.disconnect();

        if (!isSuccessfulCode(connection.getResponseCode())) {
            throw new IOException("DELETE was not successful: " + connection.getResponseCode() + " "
                                  + connection.getResponseMessage());
        }
    }

    private boolean isSuccessfulCode(int responseCode) {
        return responseCode >= 200 && responseCode < 300; // 2xx => successful
    }

    protected URL createUploadURL(ActiveMQBlobMessage message) throws JMSException, MalformedURLException {
        return new URL(transferPolicy.getUploadUrl() + message.getMessageId().toString());
    }
}
