/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.activemq.sampler;

import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.Connection;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Should be extended by the Producer and Consumer because this contains
 * similar methods and variables that they use.
 */
public class Sampler extends AbstractSampler {

    public static final String FILENAME = "Sampler.filename";
    public static final String CLASSNAME = "Sampler.classname";
    public static final String URL = "Sampler.url";
    public static final String DURATION = "Sampler.duration";
    public static final String RAMP_UP = "Sampler.ramp_up";
    public static final String NOPRODUCER = "Sampler.noprod";
    public static final String NOCONSUMER = "Sampler.noconsumer";
    public static final String NOSUBJECT = "Sampler.nosubject";
    public static final String DURABLE = "Sampler.durable";
    public static final String TOPIC = "Sampler.topic";
    public static final String TOOL_DEFAULT = "TOOL.DEFAULT";
    public static final String MSGSIZE = "Sampler.msgsize";
    public static final String MQSERVER = "Sampler.mqserver";
    public static final String DEFMSGINTERVAL = "Sampler.defmsginterval";
    public static final String MSGINTERVAL = "Sampler.msginterval";
    public static final String TRANSACTED = "Sampler.transacted";
    public static final String BATCHSIZE = "Sampler.batchsize";
    public static final String CONFIG_SUBJECT = "TOOL.DEFAULT.CONFIG";
    public static final String CONFIRM_SUBJECT = "TOOL.DEFAULT.CONFIRM";
    public static final String PUBLISH_MSG = "true";
    public static final String NOMESSAGES = "Sampler.nomessages";
    public static final String ACTIVEMQ_SERVER = JMeterUtils.getResString("activemq_server");
    public static final boolean TRANSACTED_FALSE = false;
    public static final String LAST_MESSAGE = "LAST";

    public static final int TIMEOUT = 1000;

    public static int duration;
    public static int ramp_up;

    public static boolean stopThread;

    protected static final String TCPKEY = "TCP";
    protected static final String ERRKEY = "ERR";

    protected transient SamplerClient protocolHandler;

    protected String[] subjects;
    protected String[] producers;

    protected boolean embeddedBroker = false;

    private static final Logger log = LoggingManager.getLoggerForClass();
    private static final String protoPrefix = "org.activemq.sampler.";
    private static final int DEFAULT_DURATION = 5;
    private static final int DEFAULT_RAMP_UP = 1;

    private List resources = new ArrayList();
    private Session session;
    private MessageProducer publisher;
    private Connection connection;

    /**
     * Used to populate resource.
     * @param resource
     */
    protected void addResource(Object resource) {
        resources.add(resource);
    }

    /**
     * Return the object producer class name.
     *
     * @return Returns the classname of the producer.
     */
    protected static String getClassname() {
        String className = JMeterUtils.getPropDefault("tcp.prod.handler", "SamplerClientImpl");
        return className;
    }

    /**
     * Returns a formatted string label describing this sampler
     * Example output:
     * Tcp://Tcp.nowhere.com/pub/README.txt
     *
     * @return a formatted string label describing this sampler
     */
    protected String getLabel() {
        return (this.getURL());
    }

    /**
     * @return Returns the password.
     **/
    /*protected String getPassword() {
        return getPropertyAsString(ConfigTestElement.PASSWORD);
    } */

    /**
     * Retrieves the protocol.
     *
     * @return Returns the protocol.
     */
    protected SamplerClient getProtocol() {
        SamplerClient samplerClient = null;
        Class javaClass = getClass(getClassname());

        try {
            samplerClient = (SamplerClient) javaClass.newInstance();
            if (log.isDebugEnabled()) {
                log.debug(new StringBuffer()
                          .append(this + "Created: ")
                          .append(getClassname())
                          .append("@")
                          .append(Integer.toHexString(samplerClient.hashCode())).toString());
            }
        } catch (Exception e) {
            log.error(this + " Exception creating: " + getClassname(), e);
        }

        return samplerClient;
    }

    /**
     * @return Returns the username.
     */
    protected String getUsername() {
        return getPropertyAsString(ConfigTestElement.USERNAME);
    }


    /**
     * @return Returns the timeout int object.
     */
    protected int getTimeout() {
        return TIMEOUT;
    }

    /**
     * Returns the Class object of the running producer.
     *
     * @param className
     * @return
     */
    protected Class getClass(String className) {
        Class c = null;

        try {
            c = Class.forName(className,
                              false,
                              Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {

            try {
                c = Class.forName(protoPrefix +
                                  className,
                                  false,
                                  Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e1) {
                log.error("Could not find protocol class " + className);
            }

        }

        return c;
    }

    /**
     * @param newDurable - the new durable to set.
     */
    protected void setDurable(String newDurable) {
        this.setProperty(DURABLE, newDurable);
    }

    /**
     * @return Returns whether message is durable.
     */
    protected boolean getDurable() {
        return getPropertyAsBoolean(DURABLE);
    }


    /**
     * @param newDuration - the new duration to set.
     */
    protected void setDuration(String newDuration) {
        this.setProperty(DURATION, newDuration);
    }

    /**
     * @return Returns the duration.
     */
    protected int getDuration() {
        return getPropertyAsInt(DURATION);
    }

    /**
     * @param embeddedBroker - new value.
     */
    protected void setEmbeddedBroker(boolean embeddedBroker) {
        this.embeddedBroker = embeddedBroker;
    }

    /**
     * @return Returns embeddedBroker.
     */
    protected boolean getEmbeddedBroker() {
        return embeddedBroker;
    }

    /**
     * @param newFilename - the new filename to set.
     */
    protected void setFilename(String newFilename) {
        this.setProperty(FILENAME, newFilename);
    }

    /**
     * @return Returns the filename.
     */
    protected String getFilename() {
        return getPropertyAsString(FILENAME);
    }

    /**
     * @param newMsgSize - the new message size to set.
     */
    protected void setMsgSize(String newMsgSize) {
        this.setProperty(MSGSIZE, newMsgSize);
    }

    /**
     * @return Returns the message size.
     */
    protected int getMsgSize() {
        return getPropertyAsInt(MSGSIZE);
    }

    /**
     * @param newNoCons - the number of consumer to set.
     */
    protected void setNoConsumer(String newNoCons) {
        this.setProperty(NOCONSUMER, newNoCons);
    }

    /**
     * @return Returns the number of producers.
     */
    protected int getNoConsumer() {
        return getPropertyAsInt(NOCONSUMER);
    }

    /**
     * @param newNoProd - the number of producer to set.
     */
    protected void setNoProducer(String newNoProd) {
        this.setProperty(NOPRODUCER, newNoProd);
    }

    /**
     * @return Returns the number of producers.
     */
    protected int getNoProducer() {
        return getPropertyAsInt(NOPRODUCER);
    }

    /**
     * @param newNoSubject - the new number of subject to set.
     */
    protected void setNoSubject(String newNoSubject) {
        this.setProperty(NOSUBJECT, newNoSubject);
    }

    /**
     * @return Return the number of subject.
     */
    protected int getNoSubject() {
        return getPropertyAsInt(NOSUBJECT);
    }

    /**
     * @param newRampUp - the new ramp up to set.
     */
    protected void setRampUp(String newRampUp) {
        this.setProperty(RAMP_UP, newRampUp);
    }

    /**
     * @return Returns the ramp up.
     */
    protected int getRampUp() {
        return getPropertyAsInt(RAMP_UP);
    }

    /**
     * @param newTopic - the new topic to set.
     */
    protected void setTopic(String newTopic) {
        this.setProperty(TOPIC, newTopic);
    }

    /**
     * @return Return whether the message is topic.
     */
    protected boolean getTopic() {
        return getPropertyAsBoolean(TOPIC);
    }

    /**
     * @param newURL - the new url to set.
     */
    protected void setURL(String newURL) {
        this.setProperty(URL, newURL);
    }

    /**
     * @return Returns the url.
     */
    protected String getURL() {
        return getPropertyAsString(URL);
    }

    /**
     * @param newMQServer - the new message size to set.
     */
    protected void setMQServer(String newMQServer) {
        this.setProperty(MQSERVER, newMQServer);
    }

    /**
     * @return Returns the message queue server name.
     */
    public String getMQServer() {
        return getPropertyAsString(MQSERVER);
    }

    /**
     * @param newDefMsgInterval - set to use or not the default message interval.
     */
    protected void setDefMsgInterval(String newDefMsgInterval) {
        this.setProperty(DEFMSGINTERVAL, newDefMsgInterval);
    }

    /**
     * @return Returns whether to use Default Message Interval.
     */
    protected boolean getDefMsgInterval() {
        return getPropertyAsBoolean(DEFMSGINTERVAL);
    }

    /**
     * @param newMsgInterval - the new Message Interval to set.
     */
    protected void setMsgInterval(String newMsgInterval) {
        this.setProperty(MSGINTERVAL, newMsgInterval);
    }

    /**
     * @return Returns the message interval.
     */
    protected int getMsgInterval() {
        return getPropertyAsInt(MSGINTERVAL);
    }

    /**
     *
     * @param insession - the new Session to set.
     */
    protected void setSession(Session insession) {
        session = insession;
    }

    /**
     *
     * @return Returns the session.
     */
    protected Session getSession() {
        return session;
    }

    /**
     * @return Returns whether to use Transacted type.
     */
    protected boolean getTransacted() {
        return getPropertyAsBoolean(TRANSACTED);
    }

    /**
     * @param newTransacted - when to use Transacted type.
     */
    protected void setTransacted(String newTransacted) {
        this.setProperty(TRANSACTED, newTransacted);
    }

    /**
     * @param newBatchSize - the new Batch size to set.
     */
    protected void setBatchSize(String newBatchSize) {
        this.setProperty(BATCHSIZE, newBatchSize);
    }

    /**
     * @return Returns the Batch Size.
     */
    protected int getBatchSize() {
        return getPropertyAsInt(BATCHSIZE);
    }

    /**
     *
     * @param inpublisher - MessageProducer object
     */
    protected void setPublisher(MessageProducer inpublisher) {
        publisher = inpublisher;
    }

    /**
     *
     * @return publisher - MessageProducer object.
     */
    protected MessageProducer getPublisher() {
        return publisher;
    }

    /**
     * @param inconnection - connection.
     */
    protected void setConnection(Connection inconnection) {
        connection = inconnection;
    }

    /**
     * @return Returns the connection.
     */
    protected Connection getConnection() {
        return connection;
    }

    /**
     * @param msgcount - the number of messges to send.
     */
    protected void setNoMessages(String msgcount) {
        this.setProperty(NOMESSAGES, msgcount);
    }

    /**
     * @return Returns the number of messages to send.
     */
    protected int getNoMessages() {
        return getPropertyAsInt(NOMESSAGES);
    }

    /**
     *
     * @return Return the Config Message.
     */
    protected String getConfigMessage() {
        StringBuffer sb = new StringBuffer();

        sb.append("#");
        sb.append(this.getNoMessages());
        sb.append("#");
        sb.append(this.getNoProducer());

        return sb.toString();
    }

    // provides an empty implementation, this will be properly implemented
    // by its subclass.
    public SampleResult sample(Entry entry) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    /**
     * Creates the subject that will be published.
     */
    public void start() {
        // create the subjects.
        subjects = new String[getNoSubject()];

        // appended to the subject to determine if its a queue or topic.
        String prefix = null;
        if(this.getTopic()){
            prefix = ".TOPIC";
        }else{
            prefix = ".QUEUE";
        }

        for (int i = 0; i < subjects.length; i++) {
            subjects[i] = TOOL_DEFAULT + prefix + i;
        }

        // set the duration.
        if (getDuration() == 0) {
            duration = DEFAULT_DURATION;
        } else {
            duration = getDuration();
        }

        // set the ramp_up.
        if (getRampUp() == 0) {
            ramp_up = DEFAULT_RAMP_UP;
        } else {
            ramp_up = getRampUp();
        }

        // set the thread to start.
        stopThread = false;
    }

    protected String[] getSubjects() {
       // create the subjects.
       String[] subjects = new String[getNoSubject()];

        // appended to the subject to determine if its a queue or topic.
        String prefix = null;
        if(this.getTopic()){
            prefix = ".TOPIC";
        }else{
            prefix = ".QUEUE";
        }

        for (int i = 0; i < subjects.length; i++) {
            subjects[i] = TOOL_DEFAULT + prefix + i;
        }

        return  subjects;
    }

    /**
     * the cache of TCP Connections
     */
    protected static ThreadLocal tp = new ThreadLocal() {
        protected Object initialValue() {
            return new HashMap();
        }
    };
}
