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
package org.apache.activemq.command;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.BlobMessage;
import org.apache.activemq.blob.BlobDownloader;
import org.apache.activemq.blob.BlobUploader;
import org.apache.activemq.util.JMSExceptionSupport;

/**
 * An implementation of {@link BlobMessage} for out of band BLOB transfer
 * 
 * @version $Revision: $
 * @openwire:marshaller code="29"
 */
public class ActiveMQBlobMessage extends ActiveMQMessage implements BlobMessage {
    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_BLOB_MESSAGE;

    public static final String BINARY_MIME_TYPE = "application/octet-stream";

    private String remoteBlobUrl;
    private String mimeType;
    private String name;
    private boolean deletedByBroker;

    private transient BlobUploader blobUploader;
    private transient BlobDownloader blobDownloader;
    private transient URL url;

    public Message copy() {
        ActiveMQBlobMessage copy = new ActiveMQBlobMessage();
        copy(copy);
        return copy;
    }

    private void copy(ActiveMQBlobMessage copy) {
        super.copy(copy);
        copy.setRemoteBlobUrl(getRemoteBlobUrl());
        copy.setMimeType(getMimeType());
        copy.setDeletedByBroker(isDeletedByBroker());
        copy.setBlobUploader(getBlobUploader());
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    /**
     * @openwire:property version=3 cache=false
     */
    public String getRemoteBlobUrl() {
        return remoteBlobUrl;
    }

    public void setRemoteBlobUrl(String remoteBlobUrl) {
        this.remoteBlobUrl = remoteBlobUrl;
        url = null;
    }

    /**
     * The MIME type of the BLOB which can be used to apply different content
     * types to messages.
     * 
     * @openwire:property version=3 cache=true
     */
    public String getMimeType() {
        if (mimeType == null) {
            return BINARY_MIME_TYPE;
        }
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public String getName() {
        return name;
    }

    /**
     * The name of the attachment which can be useful information if
     * transmitting files over ActiveMQ
     * 
     * @openwire:property version=3 cache=false
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @openwire:property version=3 cache=false
     */
    public boolean isDeletedByBroker() {
        return deletedByBroker;
    }

    public void setDeletedByBroker(boolean deletedByBroker) {
        this.deletedByBroker = deletedByBroker;
    }

    public String getJMSXMimeType() {
        return getMimeType();
    }

    public InputStream getInputStream() throws IOException, JMSException {
        if(blobDownloader == null) {
            return null;
        }
        return blobDownloader.getInputStream(this);
    }

    public URL getURL() throws JMSException {
        if (url == null && remoteBlobUrl != null) {
            try {
                url = new URL(remoteBlobUrl);
            } catch (MalformedURLException e) {
                throw JMSExceptionSupport.create(e);
            }
        }
        return url;
    }

    public void setURL(URL url) {
        this.url = url;
        remoteBlobUrl = url != null ? url.toExternalForm() : null;
    }

    public BlobUploader getBlobUploader() {
        return blobUploader;
    }

    public void setBlobUploader(BlobUploader blobUploader) {
        this.blobUploader = blobUploader;
    }

    public BlobDownloader getBlobDownloader() {
        return blobDownloader;
    }

    public void setBlobDownloader(BlobDownloader blobDownloader) {
        this.blobDownloader = blobDownloader;
    }

    public void onSend() throws JMSException {
        super.onSend();

        // lets ensure we upload the BLOB first out of band before we send the
        // message
        if (blobUploader != null) {
            try {
                URL value = blobUploader.upload(this);
                setURL(value);
            } catch (IOException e) {
                throw JMSExceptionSupport.create(e);
            }
        }
    }
    
    public void deleteFile() throws IOException, JMSException {
        blobDownloader.deleteFile(this);
    }
}
