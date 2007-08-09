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
package org.apache.activemq;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.JMSException;

/**
 * Represents a message which has a typically out of band Binary Large Object
 * (BLOB)
 * 
 * @version $Revision: $
 */
public interface BlobMessage extends Message {

    /**
     * Return the input stream to process the BLOB
     */
    InputStream getInputStream() throws IOException, JMSException;

    /**
     * Returns the URL for the blob if its available as an external URL (such as file, http, ftp etc)
     * or null if there is no URL available
     */
    URL getURL() throws MalformedURLException, JMSException;


    /**
     * The MIME type of the BLOB which can be used to apply different content types to messages.
     */
    String getMimeType();

    /**
     * Sets the MIME type of the BLOB so that a consumer can process things nicely with a Java Activation Framework
     * DataHandler
     */
    void setMimeType(String mimeType);


    String getName();

    /**
     * The name of the attachment which can be useful information if transmitting files over ActiveMQ
     */
    void setName(String name);
}
