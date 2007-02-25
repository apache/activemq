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

import org.apache.activemq.command.ActiveMQBlobMessage;

import javax.jms.JMSException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * A default implementation of {@link BlobUploadStrategy} which uses the URL class to upload
 * files or streams to a remote URL
 */
public class DefaultBlobUploadStrategy implements BlobUploadStrategy {
    private BlobTransferPolicy transferPolicy;

    public DefaultBlobUploadStrategy(BlobTransferPolicy transferPolicy) {
        this.transferPolicy = transferPolicy;
    }

    public URL uploadFile(ActiveMQBlobMessage message, File file) throws JMSException, IOException {
        URL url = createUploadURL(message);
        // TODO upload to URL
        // return url;
        throw new JMSException("Not implemented yet!");
    }

    public URL uploadStream(ActiveMQBlobMessage message, InputStream in) throws JMSException, IOException {
        URL url = createUploadURL(message);
        // TODO upload to URL
        // return url;
        throw new JMSException("Not implemented yet!");
    }

    protected URL createUploadURL(ActiveMQBlobMessage message) throws JMSException, MalformedURLException {
        return new URL(transferPolicy.getUploadUrl() + message.getMessageId().toString());
    }
}
