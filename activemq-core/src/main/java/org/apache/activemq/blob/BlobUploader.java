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
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;

/**
 * A helper class to represent a required upload of a BLOB to some remote URL
 * 
 * @version $Revision: $
 */
public class BlobUploader {

    private BlobTransferPolicy blobTransferPolicy;
    private File file;
    private InputStream in;

    public BlobUploader(BlobTransferPolicy blobTransferPolicy, InputStream in) {
        this.blobTransferPolicy = blobTransferPolicy;
        this.in = in;
    }

    public BlobUploader(BlobTransferPolicy blobTransferPolicy, File file) {
        this.blobTransferPolicy = blobTransferPolicy;
        this.file = file;
    }

    public URL upload(ActiveMQBlobMessage message) throws JMSException, IOException {
        if (file != null) {
            return getStrategy().uploadFile(message, file);
        } else {
            return getStrategy().uploadStream(message, in);
        }
    }

    public BlobTransferPolicy getBlobTransferPolicy() {
        return blobTransferPolicy;
    }

    public BlobUploadStrategy getStrategy() {
        return getBlobTransferPolicy().getUploadStrategy();
    }
}
