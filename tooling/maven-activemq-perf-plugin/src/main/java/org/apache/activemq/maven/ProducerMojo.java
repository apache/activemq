package org.apache.activemq.maven;

import org.apache.activemq.tool.JmsProducerSystem;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

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


/**
 * Goal which touches a timestamp file.
 *
 * @goal producer
 * @phase process
 */
public class ProducerMojo
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
     * @parameter expression="${producer.spiClass}" default-value="org.apache.activemq.tool.spi.ActiveMQPojoSPI"
     * @required
     */
    private String spiClass;

    /**
     * @parameter expression="${producer.sessTransacted}" default-value="false"
     * @required
     */
    private String sessTransacted;

    /**
     * @parameter expression="${producer.sessAckMode}" default-value="autoAck"
     * @required
     */
    private String sessAckMode;

    /**
     * @parameter expression="${producer.destName}" default-value="topic://TEST.PERFORMANCE.FOO.BAR"
     * @required
     */
    private String destName;

    /**
     * @parameter expression="${producer.destCount}" default-value="1"
     * @required
     */
    private String destCount;

    /**
     * @parameter expression="${producer.destComposite}" default-value="false"
     * @required
     */
    private String destComposite;

    /**
     * @parameter expression="${producer.messageSize}" default-value="1024"
     * @required
     */
    private String messageSize;

    /**
     * @parameter expression="${producer.sendCount}" default-value="1000000"
     * @required
     */
    private String sendCount;

    /*
     * @parameter expression="${producer.sendDuration}" default-value="60000"
     * @required

    private String sendDuration;
    */

    /**
     * @parameter expression="${producer.sendType}" default-value="time"
     * @required
     */
    private String sendType;

    /**
     * @parameter expression="${factory.brokerUrl}" default-value="tcp://localhost:61616"
     * @required
     */
    private String brokerUrl;

    /**
     * @parameter expression="${factory.asyncSend}" default-value="true"
     * @required
     */
    private String asyncSend;

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

        JmsProducerSystem.main(createArgument());
    }

    public String[] createArgument() {

        String[] options = {
            "sampler.duration=" + duration,   
            "sampler.interval=" + interval,     
            "sampler.rampUpTime=" + rampUpTime,   
            "sampler.rampDownTime=" + rampDownTime, 
    
            "producer.spiClass=" + spiClass,
            "producer.sessTransacted=" + sessTransacted,
            "producer.sessAckMode=" + sessAckMode,
            "producer.destName=" + destName,
            "producer.destCount=" + destCount,
            "producer.destComposite=" + destComposite,
    
            "producer.messageSize="+messageSize,
            "producer.sendCount="+sendCount,    
            "producer.sendDuration="+duration, 
            "producer.sendType="+sendType,
    
            "factory.brokerUrl="+brokerUrl,
            "factory.asyncSend="+asyncSend,
    
            "sysTest.numClients=" + numClients,
            "sysTest.totalDests=" + totalDests,
            "sysTest.destDistro=" + destDistro,
            "sysTest.reportDirectory=" + reportDirectory 
        };

        return options;
    }
}
