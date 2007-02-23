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
import java.io.*;
import java.io.IOException;
import java.net.URL;

/**
 * A helper class to represent a required upload of a BLOB to some remote URL
 *
 * @version $Revision: $
 */
public class BlobUploader {

    private BlobUploadStrategy strategy;
    private File file;
    private InputStream in;


    public BlobUploader(BlobUploadStrategy strategy, File file) {
        this.strategy = strategy;
        this.file = file;
    }

    public BlobUploader(BlobUploadStrategy strategy, InputStream in) {
        this.strategy = strategy;
        this.in = in;
    }

    public URL upload(ActiveMQBlobMessage message) throws JMSException, IOException {
        if (file != null) {
            return strategy.uploadFile(message, file);
        }
        else {
            return strategy.uploadStream(message, in);
        }
    }
}
