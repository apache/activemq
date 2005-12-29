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
package org.apache.activemq.network.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;

import org.apache.activemq.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;
/**
 * A Destination bridge is used to bridge between to different JMS systems
 * 
 * @version $Revision: 1.1.1.1 $
 */
abstract class DestinationBridge implements Service,MessageListener{
    private static final Log log=LogFactory.getLog(DestinationBridge.class);
    protected MessageConsumer consumer;
    protected AtomicBoolean started=new AtomicBoolean(false);
    protected JmsMesageConvertor jmsMessageConvertor;
    protected boolean doHandleReplyTo = true;

    /**
     * @return Returns the consumer.
     */
    public MessageConsumer getConsumer(){
        return consumer;
    }

    /**
     * @param consumer
     *            The consumer to set.
     */
    public void setConsumer(MessageConsumer consumer){
        this.consumer=consumer;
    }

    /**
     * @return Returns the jmsMessageConvertor.
     */
    public JmsMesageConvertor getJmsMessageConvertor(){
        return jmsMessageConvertor;
    }

    /**
     * @param jmsMessageConvertor
     *            The jmsMessageConvertor to set.
     */
    public void setJmsMessageConvertor(JmsMesageConvertor jmsMessageConvertor){
        this.jmsMessageConvertor=jmsMessageConvertor;
    }

    public void start() throws Exception{
        if(started.compareAndSet(false,true)){
            MessageConsumer consumer=createConsumer();
            consumer.setMessageListener(this);
            createProducer();
        }
    }

    public void stop() throws Exception{
        started.set(false);
    }

    public void onMessage(Message message){
        if(started.get()&&message!=null){
            try{
                if(doHandleReplyTo){
                    Destination replyTo=message.getJMSReplyTo();
                    if(replyTo!=null){
                        replyTo=processReplyToDestination(replyTo);
                        message.setJMSReplyTo(replyTo);
                    }
                }else {
                    message.setJMSReplyTo(null);
                }
                Message converted=jmsMessageConvertor.convert(message);
                sendMessage(converted);
                message.acknowledge();
            }catch(JMSException e){
                log.error("failed to forward message: "+message,e);
                try{
                    stop();
                }catch(Exception e1){
                    log.warn("Failed to stop cleanly",e1);
                }
            }
        }
    }
    
    /**
     * @return Returns the doHandleReplyTo.
     */
    protected boolean isDoHandleReplyTo(){
        return doHandleReplyTo;
    }

    /**
     * @param doHandleReplyTo The doHandleReplyTo to set.
     */
    protected void setDoHandleReplyTo(boolean doHandleReplyTo){
        this.doHandleReplyTo=doHandleReplyTo;
    }

    protected abstract MessageConsumer createConsumer() throws JMSException;

    protected abstract MessageProducer createProducer() throws JMSException;

    protected abstract void sendMessage(Message message) throws JMSException;

    protected abstract Destination processReplyToDestination(Destination destination);

    
}