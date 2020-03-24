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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.commons.net.ftp.FTPClient;

/**
 * A FTP implementation for {@link BlobDownloadStrategy}.
 */
public class FTPBlobDownloadStrategy extends FTPStrategy implements BlobDownloadStrategy {

    public FTPBlobDownloadStrategy(BlobTransferPolicy transferPolicy) throws MalformedURLException {
        super(transferPolicy);
    }

    public InputStream getInputStream(ActiveMQBlobMessage message) throws IOException, JMSException {
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

        url = message.getURL();
        final FTPClient ftp = createFTP();
        String path = url.getPath();
        String workingDir = path.substring(0, path.lastIndexOf("/"));
        String file = path.substring(path.lastIndexOf("/") + 1);
        ftp.changeWorkingDirectory(workingDir);
        ftp.setFileType(FTPClient.BINARY_FILE_TYPE);

        InputStream input = new FilterInputStream(ftp.retrieveFileStream(file)) {

            public void close() throws IOException {
                in.close();
                ftp.quit();
                ftp.disconnect();
            }
        };

        return input;
    }

    public void deleteFile(ActiveMQBlobMessage message) throws IOException, JMSException {
        url = message.getURL();
        final FTPClient ftp = createFTP();

        String path = url.getPath();
        try {
            if (!ftp.deleteFile(path)) {
                throw new JMSException("Delete file failed: " + ftp.getReplyString());
            }
        } finally {
            ftp.quit();
            ftp.disconnect();
        }

    }

}
