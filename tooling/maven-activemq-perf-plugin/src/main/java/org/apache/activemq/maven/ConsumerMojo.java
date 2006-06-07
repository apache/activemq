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
     * @parameter expression="${sampler.interval}" default-value="5000"
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
     * @parameter expression="${client.spiClass}" default-value="org.apache.activemq.tool.spi.ActiveMQPojoSPI"
     * @required
     */
    private String spiClass;

    /**
     * @parameter expression="${client.sessTransacted}" default-value="false"
     * @required
     */
    private String sessTransacted;

    /**
     * @parameter expression="${client.sessAckMode}" default-value="autoAck"
     * @required
     */
    private String sessAckMode;

    /**
     * @parameter expression="${client.destName}" default-value="topic://FOO.BAR.TEST"
     * @required
     */
    private String destName;

    /**
     * @parameter expression="${client.destCount}" default-value="1"
     * @required
     */
    private String destCount;

    /**
     * @parameter expression="${client.destComposite}" default-value="false"
     * @required
     */
    private String destComposite;

    /**
     * @parameter expression="${client.durable}" default-value="false"
     * @required
     */
    private String durable;

    /**
     * @parameter expression="${client.asyncRecv}" default-value="true"
     * @required
     */
    private String asyncRecv;

    /**
     * @parameter expression="${client.recvCount}" default-value="1000"
     * @required
     */
    private String recvCount;

    /*
     * @parameter expression="${client.recvDuration}" default-value="60000"
     * @required

    private String recvDuration;
    */

    /**
     * @parameter expression="${client.recvType}" default-value="time"
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
     * @parameter expression="${factory.prefetchQueue}" default-value="10"
     * @required
     */
    private String prefetchQueue;

    /**
     * @parameter expression="${factory.prefetchTopic}" default-value="10"
     * @required
     */
    private String prefetchTopic;

    /**
     * @parameter expression="${factory.useRetroactive}" default-value="false"
     * @required
     */
    private String useRetroactive;

    /**
     * @parameter expression="${sysTest.numClients}" default-value="5"
     * @required
     */
    private String numClients;

    /**
     * @parameter expression="${sysTest.totalDests}" default-value="5"
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
        String[] options = new String[25];

        options[0] = "sampler.duration=" + duration;     // 1 min
        options[1] = "sampler.interval=" + interval;      // 5 secs
        options[2] = "sampler.rampUpTime=" + rampUpTime;   // 10 secs
        options[3] = "sampler.rampDownTime=" + rampDownTime; // 10 secs

        options[4] = "client.spiClass=" + spiClass;
        options[5] = "client.sessTransacted=" + sessTransacted;
        options[6] = "client.sessAckMode=" + sessAckMode;
        options[7] = "client.destName=" + destName;
        options[8] = "client.destCount=" + destCount;
        options[9] = "client.destComposite=" + destComposite;

        options[10] = "client.durable=" + durable;
        options[11] = "client.asyncRecv=" + asyncRecv;
        options[12] = "client.recvCount=" + recvCount;     // 1000 messages
        options[13] = "client.recvDuration=" + duration; // use sampler.duration.
        options[14] = "client.recvType=" + recvType;

        options[15] = "factory.brokerUrl=" + brokerUrl;
        options[16] = "factory.optimAck=" + optimAck;
        options[17] = "factory.optimDispatch=" + optimDispatch;
        options[18] = "factory.prefetchQueue=" + prefetchQueue;
        options[19] = "factory.prefetchTopic=" + prefetchTopic;
        options[20] = "factory.useRetroactive=" + useRetroactive;

        options[21] = "sysTest.numClients=" + numClients;
        options[22] = "sysTest.totalDests=" + totalDests;
        options[23] = "sysTest.destDistro=" + destDistro;
        options[24] = "sysTest.reportDirectory=" + reportDirectory;

        return options;
    }
}
