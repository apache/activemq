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
     * @parameter expression="${producer.messageSize}" default-value="1024"
     * @required
     */
    private String messageSize;

    /**
     * @parameter expression="${producer.sendCount}" default-value="1000"
     * @required
     */
    private String sendCount;

    /**
     * @parameter expression="${producer.sendDuration}" default-value="60000"
     * @required
     */
    private String sendDuration;

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

        JmsProducerSystem.main(createArgument());
    }

    public String[] createArgument() {

        String[] options = new String[20];
        options[0] = "-Dsampler.duration=" + duration;     // 1 min
        options[1] = "-Dsampler.interval=" + interval;      // 5 secs
        options[2] = "-Dsampler.rampUpTime=" + rampUpTime;   // 10 secs
        options[3] = "-Dsampler.rampDownTime=" + rampDownTime; // 10 secs

        options[4] = "-Dclient.spiClass=" + spiClass;
        options[5] = "-Dclient.sessTransacted=" + sessTransacted;
        options[6] = "-Dclient.sessAckMode=" + sessAckMode;
        options[7] = "-Dclient.destName=" + destName;
        options[8] = "-Dclient.destCount=" + destCount;
        options[9] = "-Dclient.destComposite=" + destComposite;

        options[10] = "-Dproducer.messageSize="+messageSize;
        options[11] = "-Dproducer.sendCount="+sendCount;     // 1000 messages
        options[12] = "-Dproducer.sendDuration="+sendDuration; // 1 min
        options[13] = "-Dproducer.sendType="+sendType;

        options[14] = "-Dfactory.brokerUrl="+brokerUrl;
        options[15] = "-Dfactory.asyncSend="+asyncSend;

        options[16] = "-DsysTest.numClients=" + numClients;
        options[17] = "-DsysTest.totalDests=" + totalDests;
        options[18] = "-DsysTest.destDistro=" + destDistro;
        options[19] = "-DsysTest.reportDirectory=" + reportDirectory;

        return options;
    }
}
