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

package org.apache.activemq.store;

import java.io.IOException;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.MessageId;

/**
 * Represents a message store which is used by the persistent 
 * implementations
 * 
 * @version $Revision: 1.5 $
 */
public interface ReferenceStore extends MessageStore {

	public class ReferenceData {
		long expiration;
		int fileId;
		int offset;
		
		public long getExpiration() {
			return expiration;
		}
		public void setExpiration(long expiration) {
			this.expiration = expiration;
		}
		public int getFileId() {
			return fileId;
		}
		public void setFileId(int file) {
			this.fileId = file;
		}
		public int getOffset() {
			return offset;
		}
		public void setOffset(int offset) {
			this.offset = offset;
		}
		
		@Override
		public String toString() {
			return "ReferenceData fileId="+fileId+", offset="+offset+", expiration="+expiration;
		}
	}
	
    /**
     * Adds a message reference to the message store
     */
    public void addMessageReference(ConnectionContext context, MessageId messageId, ReferenceData data) throws IOException;

    /**
     * Looks up a message using either the String messageID or the messageNumber. Implementations are encouraged to fill
     * in the missing key if its easy to do so.
     */
    public ReferenceData getMessageReference(MessageId identity) throws IOException;
    
    /**
     * @return true if it supports external batch control
     */
    public boolean supportsExternalBatchControl();
    
    public void setBatch(MessageId startAfter);
    
}
