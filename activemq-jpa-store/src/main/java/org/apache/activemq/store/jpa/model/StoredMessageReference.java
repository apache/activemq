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
package org.apache.activemq.store.jpa.model;

import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.apache.openjpa.persistence.jdbc.Index;

/** 
 */
@Entity()
public class StoredMessageReference {

    @Id
    private long id;

    @Basic(optional = false)
    @Index(enabled = true, unique = false)
    private String messageId;

    @Basic(optional = false)
    @Index(enabled = true, unique = false)
    private String destination;

    @Basic
    @Index(enabled = false, unique = false)
    private long exiration;

    @Basic(optional = false)
    @Index(enabled = false, unique = false)
    private int fileId;

    @Basic(optional = false)
    @Index(enabled = false, unique = false)
    private int offset;

    public StoredMessageReference() {
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public long getExiration() {
        return exiration;
    }

    public void setExiration(long exiration) {
        this.exiration = exiration;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public long getId() {
        return id;
    }

    public void setId(long sequenceId) {
        this.id = sequenceId;
    }

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

}
