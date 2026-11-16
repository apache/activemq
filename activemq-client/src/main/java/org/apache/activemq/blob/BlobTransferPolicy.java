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
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The policy for configuring how BLOBs (Binary Large OBjects) are transferred
 * out of band between producers, brokers and consumers.
 *
 * 
 */
public class BlobTransferPolicy {

    public static final Set<String> DEFAULT_ALLOWED_PROTOCOLS = Set.of("file", "https");

    /**
     * Comma separated list of protocols that replaces the defaults.
     */
    public static final String ALLOWED_PROTOCOLS_PROPERTY = "org.apache.activemq.blob.AllowedProtocols";

    /**
     * Set to false to stop following HTTP redirects.
     */
    public static final String HTTP_FOLLOW_REDIRECTS_PROPERTY = "org.apache.activemq.blob.HttpFollowRedirects";

    private String defaultUploadUrl = "https://localhost:8080/uploads/";
    private String brokerUploadUrl;
    private String uploadUrl;
    private int bufferSize = 128 * 1024;
    private BlobUploadStrategy uploadStrategy;
    private BlobDownloadStrategy downloadStrategy;

    /**
     * Returns a copy of this policy object
     */
    public BlobTransferPolicy copy() {
        BlobTransferPolicy that = new BlobTransferPolicy();
        that.defaultUploadUrl = this.defaultUploadUrl;
        that.brokerUploadUrl = this.brokerUploadUrl;
        that.uploadUrl = this.uploadUrl;
        that.bufferSize = this.bufferSize;
        that.uploadStrategy = this.uploadStrategy;
        that.downloadStrategy = this.downloadStrategy;
        return that;
    }

    public String getUploadUrl() {
        if (uploadUrl == null) {
            uploadUrl = getBrokerUploadUrl();
            if (uploadUrl == null) {
                uploadUrl = getDefaultUploadUrl();
            }
        }
        return uploadUrl;
    }

    /**
     * Sets the upload URL to use explicitly on the client which will
     * overload the default or the broker's URL. This allows the client to decide
     * where to upload files to irrespective of the brokers configuration.
     */
    public void setUploadUrl(String uploadUrl) {
        this.uploadUrl = uploadUrl;
    }

    public String getBrokerUploadUrl() {
        return brokerUploadUrl;
    }

    /**
     * Called by the JMS client when a broker advertises its upload URL
     */
    public void setBrokerUploadUrl(String brokerUploadUrl) {
        this.brokerUploadUrl = brokerUploadUrl;
    }

    public String getDefaultUploadUrl() {
        return defaultUploadUrl;
    }

    /**
     * Sets the default upload URL to use if the broker does not
     * have a configured upload URL
     */
    public void setDefaultUploadUrl(String defaultUploadUrl) {
        this.defaultUploadUrl = defaultUploadUrl;
    }

    public BlobUploadStrategy getUploadStrategy() {
        if (uploadStrategy == null) {
            uploadStrategy = createUploadStrategy();
        }
        return uploadStrategy;
    }

    public BlobDownloadStrategy getDownloadStrategy() {
        if(downloadStrategy == null) {
            downloadStrategy = createDownloadStrategy();
        }
        return downloadStrategy;
    }

    /**
     * Sets the upload strategy to use for uploading BLOBs to some URL
     */
    public void setUploadStrategy(BlobUploadStrategy uploadStrategy) {
        this.uploadStrategy = uploadStrategy;
    }

    /**
     * Sets the download strategy to use for downloading BLOBs from some URL
     */
    public void setDownloadStrategy(BlobDownloadStrategy downloadStrategy) {
        this.downloadStrategy = downloadStrategy;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Sets the default buffer size used when uploading or downloading files
     */
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public static boolean isHttpFollowRedirects() {
        final String configured = System.getProperty(HTTP_FOLLOW_REDIRECTS_PROPERTY);
        return configured == null || Boolean.parseBoolean(configured.trim());
    }

    public static Set<String> getAllowedProtocols() {
        final String configured = System.getProperty(ALLOWED_PROTOCOLS_PROPERTY);
        if (configured == null) {
            return DEFAULT_ALLOWED_PROTOCOLS;
        }
        return Arrays.stream(configured.split(","))
            .map(protocol -> protocol.trim().toLowerCase(Locale.ROOT))
            .filter(protocol -> !protocol.isEmpty())
            .collect(Collectors.toUnmodifiableSet());
    }

    public static void checkProtocolAllowed(String protocol) throws IOException {
        final String name = protocol == null ? "" : protocol.toLowerCase(Locale.ROOT);
        if (!getAllowedProtocols().contains(name)) {
            throw new IOException("The URL protocol '" + protocol + "' is not allowed for blob transfer."
                + " Allowed protocols are set with " + ALLOWED_PROTOCOLS_PROPERTY);
        }
    }

    /**
     * Returns the upload strategy for the upload URL: the file system
     * strategy for a file URL, otherwise the default strategy, which
     * supports http and https.
     * 
     * @return
     */
    protected BlobUploadStrategy createUploadStrategy() {
    	BlobUploadStrategy strategy;
    	try {
            URL url = new URL(getUploadUrl());

            if (url.getProtocol().equalsIgnoreCase("FILE")) {
                strategy = new FileSystemBlobStrategy(this);
            } else {
                strategy = new DefaultBlobUploadStrategy(this);
            }
        } catch (MalformedURLException e) {
            strategy = new DefaultBlobUploadStrategy(this);
        } catch (URISyntaxException e) {
            strategy = new DefaultBlobUploadStrategy(this);
        }
        return strategy;
    }
    
    /**
     * Returns the download strategy for the upload URL: the file system
     * strategy for a file URL, otherwise the default strategy, which
     * supports http and https.
     * 
     * @return
     */
    protected BlobDownloadStrategy createDownloadStrategy() {
        BlobDownloadStrategy strategy;
        try {
            URL url = new URL(getUploadUrl());
            
            if (url.getProtocol().equalsIgnoreCase("FILE")) {
                strategy = new FileSystemBlobStrategy(this);
            } else {
                strategy = new DefaultBlobDownloadStrategy(this);
            }
        } catch (MalformedURLException e) {
            strategy = new DefaultBlobDownloadStrategy(this);
        } catch (URISyntaxException e) {
            strategy = new DefaultBlobDownloadStrategy(this);
        }
        return strategy;
    }

    
}
