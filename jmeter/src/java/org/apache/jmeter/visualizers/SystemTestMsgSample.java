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
