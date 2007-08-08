/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.activemq.store.kahadaptor;

import org.apache.activemq.store.ReferenceStore.ReferenceData;

public class ReferenceRecord {

    private String messageId;
    private ReferenceData data;

    public ReferenceRecord() {
    }

    public ReferenceRecord(String messageId, ReferenceData data) {
        this.messageId = messageId;
        this.data = data;
    }

    /**
     * @return the data
     */
    public ReferenceData getData() {
        return this.data;
    }

    /**
     * @param data the data to set
     */
    public void setData(ReferenceData data) {
        this.data = data;
    }

    /**
     * @return the messageId
     */
    public String getMessageId() {
        return this.messageId;
    }

    /**
     * @param messageId the messageId to set
     */
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String toString() {
        return "ReferenceRecord(id=" + messageId + ",data=" + data + ")";
    }
}
