/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jmeter.visualizers;

import java.io.Serializable;

public class SystemTestMsgSample implements Serializable, Comparable {
    public String consumerid;
    public String prodName;
    public String msgBody;
    public Integer producerSeqID;
    public Integer consumerSeqID;

    public SystemTestMsgSample(String consumerid, String prodName, String msgBody, Integer prodSeq, Integer conSeq) {
        this.consumerid = consumerid;
        this.prodName = prodName;
        this.msgBody = msgBody;
        this.producerSeqID = prodSeq;
        this.consumerSeqID = conSeq;
    }

    /**
     *
     * @return get consumer identifier.
     */
    public String getConsumerID() {
        return consumerid;
    }

    /**
     *
     * @param consumerid  - sets consumer identifier
     */
    public void setConsumerID(String consumerid) {
        this.consumerid = consumerid;
    }

    /**
     *
     * @return Returns the Producer name.
     */
    public String getProdName() {
        return prodName;
    }

    /**
     *
     * @param prodName - The Producer name to set.
     */
    public void setProdName(String prodName) {
        this.prodName = prodName;
    }

    /**
     *
     * @return Return the Producer message.
     */
    public String getMsgBody() {
        return msgBody;
    }

    /**
     *
     * @param msgBody - The Producer message to set.
     */
    public void setMSgBody(String msgBody) {
        this.msgBody = msgBody;
    }

    /**
     *
     * @return Returns the current producer message count.
     */
    public Integer getProducerSeq() {
        return producerSeqID;
    }

    /**
     *
     * @param count - The message count to set.
     */
    public void setProducerSeq(Integer count) {
        this.producerSeqID = count;
    }

    /**
     *
     * @return Returns the current consumer message count.
     */
    public Integer getConsumerSeq() {
        return consumerSeqID;
    }

    /**
     *
     * @param count - The message count to set.
     */
    public void setConsumerSeq(Integer count) {
        this.consumerSeqID = count;
    }


    public int compareTo(Object o) {
        return -1;
    }

}
