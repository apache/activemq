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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;

/**
 * {@link BlobUploadStrategy} and {@link BlobDownloadStrategy} implementation which use the local filesystem for storing
 * the payload
 *
 */
public class FileSystemBlobStrategy implements BlobUploadStrategy, BlobDownloadStrategy{


    private final BlobTransferPolicy policy;
    private File rootFile;

    public FileSystemBlobStrategy(final BlobTransferPolicy policy) throws MalformedURLException, URISyntaxException  {
        this.policy = policy;

        createRootFolder();
    }

    /**
     * Create the root folder if not exist
     *
     * @throws MalformedURLException
     * @throws URISyntaxException
     */
    protected void createRootFolder() throws MalformedURLException, URISyntaxException {
        rootFile = new File(new URL(policy.getUploadUrl()).toURI());
        if (rootFile.exists() == false) {
            rootFile.mkdirs();
        } else if (rootFile.isDirectory() == false) {
            throw new IllegalArgumentException("Given url is not a directory " + rootFile );
        }
    }
    /*
     * (non-Javadoc)
     * @see org.apache.activemq.blob.BlobUploadStrategy#uploadFile(org.apache.activemq.command.ActiveMQBlobMessage, java.io.File)
     */
    public URL uploadFile(ActiveMQBlobMessage message, File file) throws JMSException, IOException {
        try(FileInputStream fis = new FileInputStream(file)) {
            return uploadStream(message, fis);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.activemq.blob.BlobUploadStrategy#uploadStream(org.apache.activemq.command.ActiveMQBlobMessage, java.io.InputStream)
     */
    public URL uploadStream(ActiveMQBlobMessage message, InputStream in) throws JMSException, IOException {
        File f = getFile(message);
        try(FileOutputStream out = new FileOutputStream(f)) {
            byte[] buffer = new byte[policy.getBufferSize()];
            for (int c = in.read(buffer); c != -1; c = in.read(buffer)) {
                out.write(buffer, 0, c);
                out.flush();
            }
        }
        // File.toURL() is deprecated
        return f.toURI().toURL();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.activemq.blob.BlobDownloadStrategy#deleteFile(org.apache.activemq.command.ActiveMQBlobMessage)
     */
    public void deleteFile(ActiveMQBlobMessage message) throws IOException, JMSException {
        File f = getFile(message);
        if (f.exists()) {
            if (f.delete() == false) throw new IOException("Unable to delete file " + f);
        }
    }

    /**
     * Returns a {@link FileInputStream} for the give {@link ActiveMQBlobMessage}
     */
    public InputStream getInputStream(ActiveMQBlobMessage message) throws IOException, JMSException {
        return new FileInputStream(getFile(message));
    }


    /**
     * Return the {@link File} for the {@link ActiveMQBlobMessage}.
     *
     * @param message
     * @return file
     * @throws JMSException
     * @throws IOException
     */
    protected File getFile(ActiveMQBlobMessage message) throws JMSException, IOException {
    	if (message.getURL() != null) {
    		try {
				return new File(message.getURL().toURI());
			} catch (URISyntaxException e) {
                                IOException ioe = new IOException("Unable to open file for message " + message);
                                ioe.initCause(e);
			}
    	}
        //replace all : with _ to make windows more happy
        String fileName = message.getJMSMessageID().replaceAll(":", "_");
        return new File(rootFile, fileName);

    }
}
