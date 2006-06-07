package org.apache.activemq.maven;

/*
 * Copyright 2001-2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.activemq.tool.JmsConsumerSystem;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

import javax.jms.JMSException;


/**
 * Goal which touches a timestamp file.
 *
 * @goal consumer
 * @phase process-sources
 */
public class ConsumerMojo
        extends AbstractMojo {

    /**
     * @parameter expression="${sampler.duration}" default-value="60000"
     * @required
     */
    private String duration;

    /**
     * @parameter expression="${sampler.interval}" default-value="1000"
     * @required
     */
    private String interval;

    /**
     * @parameter expression="${sampler.rampUpTime}" default-value="10000"
     * @required
     */
    private String rampUpTime;

    /**
     * @parameter expression="${sampler.rampDownTime}" default-value="10000"
     * @required
     */
    private String rampDownTime;

    /**
     * @parameter expression="${consumer.spiClass}" default-value="org.apache.activemq.tool.spi.ActiveMQPojoSPI"
     * @required
     */
    private String spiClass;

    /**
     * @parameter expression="${consumer.sessTransacted}" default-value="false"
     * @required
     */
    private String sessTransacted;

    /**
     * @parameter expression="${consumer.sessAckMode}" default-value="autoAck"
     * @required
     */
    private String sessAckMode;

    /**
     * @parameter expression="${consumer.destName}" default-value="topic://TEST.PERFORMANCE.FOO.BAR"
     * @required
     */
    private String destName;

    /**
     * @parameter expression="${consumer.destCount}" default-value="1"
     * @required
     */
    private String destCount;

    /**
     * @parameter expression="${consumer.destComposite}" default-value="false"
     * @required
     */
    private String destComposite;

    /**
     * @parameter expression="${consumer.durable}" default-value="false"
     * @required
     */
    private String durable;

    /**
     * @parameter expression="${consumer.asyncRecv}" default-value="true"
     * @required
     */
    private String asyncRecv;

    /**
     * @parameter expression="${consumer.recvCount}" default-value="1000000"
     * @required
     */
    private String recvCount;

    /*
     * @parameter expression="${consumer.recvDuration}" default-value="60000"
     * @required

    private String recvDuration;
    */

    /**
     * @parameter expression="${consumer.recvType}" default-value="time"
     * @required
     */
    private String recvType;

    /**
     * @parameter expression="${factory.brokerUrl}" default-value="tcp://localhost:61616"
     * @required
     */
    private String brokerUrl;

    /**
     * @parameter expression="${factory.optimAck}" default-value="true"
     * @required
     */
    private String optimAck;

    /**
     * @parameter expression="${factory.optimDispatch}" default-value="true"
     * @required
     */
    private String optimDispatch;

    /**
     * @parameter expression="${factory.prefetchQueue}" default-value="5000"
     * @required
     */
    private String prefetchQueue;

    /**
     * @parameter expression="${factory.prefetchTopic}" default-value="5000"
     * @required
     */
    private String prefetchTopic;

    /**
     * @parameter expression="${factory.useRetroactive}" default-value="false"
     * @required
     */
    private String useRetroactive;

    /**
     * @parameter expression="${sysTest.numClients}" default-value="1"
     * @required
     */
    private String numClients;

    /**
     * @parameter expression="${sysTest.totalDests}" default-value="1"
     * @required
     */
    private String totalDests;

    /**
     * @parameter expression="${sysTest.destDistro}" default-value="all"
     * @required
     */
    private String destDistro;

    /**
     * @parameter expression="${sysTest.reportDirectory}" default-value="${project.build.directory}/test-perf"
     * @required
     */
    private String reportDirectory;

    public void execute()
            throws MojoExecutionException {

        try {
            JmsConsumerSystem.main(createArgument());
        } catch (JMSException e) {
            throw new MojoExecutionException(e.getMessage());
        }

    }

    public String[] createArgument() {
        String[] options = {
            "sampler.duration=" + duration, 
            "sampler.interval=" + interval,    
            "sampler.rampUpTime=" + rampUpTime,   
            "sampler.rampDownTime=" + rampDownTime, 
    
            "consumer.spiClass=" + spiClass,
            "consumer.sessTransacted=" + sessTransacted,
            "consumer.sessAckMode=" + sessAckMode,
            "consumer.destName=" + destName,
            "consumer.destCount=" + destCount,
            "consumer.destComposite=" + destComposite,
    
            "consumer.durable=" + durable,
            "consumer.asyncRecv=" + asyncRecv,
            "consumer.recvCount=" + recvCount,   
            "consumer.recvDuration=" + duration, 
            "consumer.recvType=" + recvType,
    
            "factory.brokerUrl=" + brokerUrl,
            "factory.optimAck=" + optimAck,
            "factory.optimDispatch=" + optimDispatch,
            "factory.prefetchQueue=" + prefetchQueue,
            "factory.prefetchTopic=" + prefetchTopic,
            "factory.useRetroactive=" + useRetroactive,
    
            "sysTest.numClients=" + numClients,
            "sysTest.totalDests=" + totalDests,
            "sysTest.destDistro=" + destDistro,
            "sysTest.reportDirectory=" + reportDirectory
        };

        return options;
    }
}
