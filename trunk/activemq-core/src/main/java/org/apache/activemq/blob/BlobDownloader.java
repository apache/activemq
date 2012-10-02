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
import javax.jms.JMSException;
import org.apache.activemq.command.ActiveMQBlobMessage;


/**
 * Mediator for Blob Download
 */
public class BlobDownloader {

    private final BlobTransferPolicy blobTransferPolicy;
    
    public BlobDownloader(BlobTransferPolicy transferPolicy) {
        // need to do a defensive copy
        this.blobTransferPolicy = transferPolicy.copy();
    }
    
    public InputStream getInputStream(ActiveMQBlobMessage message) throws IOException, JMSException {
        return getStrategy().getInputStream(message);
    }
    
    public void deleteFile(ActiveMQBlobMessage message) throws IOException, JMSException {
        getStrategy().deleteFile(message);
    }
    
    public BlobTransferPolicy getBlobTransferPolicy() {
        return blobTransferPolicy;
    }
    
    public BlobDownloadStrategy getStrategy() {
        return getBlobTransferPolicy().getDownloadStrategy();
    }
}
