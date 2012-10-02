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
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.commons.net.ftp.FTPClient;

/**
 * A FTP implementation of {@link BlobUploadStrategy}.
 */
public class FTPBlobUploadStrategy extends FTPStrategy implements BlobUploadStrategy {
	
	public FTPBlobUploadStrategy(BlobTransferPolicy transferPolicy) throws MalformedURLException {
		super(transferPolicy);
	}

	public URL uploadFile(ActiveMQBlobMessage message, File file) throws JMSException, IOException {
		return uploadStream(message, new FileInputStream(file));
	}

	public URL uploadStream(ActiveMQBlobMessage message, InputStream in)
			throws JMSException, IOException {

	    FTPClient ftp = createFTP();
	    try {
    		String path = url.getPath();
            String workingDir = path.substring(0, path.lastIndexOf("/"));
    		String filename = message.getMessageId().toString().replaceAll(":", "_");
            ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
            
            String url;
            if(!ftp.changeWorkingDirectory(workingDir)) {
            	url = this.url.toString().replaceFirst(this.url.getPath(), "")+"/";
            } else {
            	url = this.url.toString();
            }
            
    		if (!ftp.storeFile(filename, in)) {
    		    throw new JMSException("FTP store failed: " + ftp.getReplyString());
    		}
    		return new URL(url + filename);
	    } finally {
    		ftp.quit();
    		ftp.disconnect();
	    }
		
	}

}
