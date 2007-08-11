package org.apache.activemq.maven;

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


import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.activemq.tool.JMSMemtest;

import javax.jms.JMSException;


/**
 * Goal which does a memory usage test  to check for any memory leak
 *
 * @goal memtest
 * @phase process-sources
 */
public class MemtestMojo
        extends AbstractMojo {

    /**
     * @parameter expression="${url} 
     *
     */
    private String url;

    /**
     * @parameter expression="${topic}" default-value="true"
     * @required
     */
    private String topic;

    /**
     * @parameter expression="${connectionCheckpointSize}"  default-value="-1"
     * @required
     */
    private String connectionCheckpointSize;

    /**
     * @parameter expression="${durable}" default-value="false"
     * @required
     */
    private String durable;

    /**
     * @parameter expression="${producerCount}" default-value="1"
     * @required
     */
    private String producerCount;

    /**
     * @parameter expression="${prefetchSize}" default-value="-1"
     * @required
     */
    private String prefetchSize;


    /**
     * @parameter expression="${consumerCount}" default-value="1"
     * @required
     */
    private String consumerCount;

    /**
     * @parameter expression="${messageCount}" default-value="100000"
     * @required
     */
    private String messageCount;

    /**
     * @parameter expression="${messageSize}" default-value="10240"
     * @required
     */
    private String messageSize;

    /**
     * @parameter expression="${checkpointInterval}" default-value="2"
     * @required
     */
    private String checkpointInterval;

    /**
     * @parameter expression="${destinationName}" default-value="FOO.BAR"
     * @required
     */
    private String destinationName;

    /**
     * @parameter expression="${reportName}" default-value="activemq-memory-usage-report"
     * @required
     */
    private String reportName;

    /**
     * @parameter expression="${reportDirectory}" default-value="${project.build.directory}/test-memtest"
     * @required
     */
    private String reportDirectory;



    public void execute()
            throws MojoExecutionException {

        JMSMemtest.main(createArgument());
    }



    public String[] createArgument() {


        String[] options = {
            "url=" + url,
            "topic=" + topic,
            "durable=" + durable,
            "connectionCheckpointSize=" + connectionCheckpointSize,
            "producerCount=" + producerCount,
            "consumerCount=" + consumerCount,
            "messageCount=" + messageCount,
            "messageSize=" + messageSize,
            "checkpointInterval=" + checkpointInterval,
            "destinationName=" + destinationName,
            "reportName=" + reportName,
            "prefetchSize=" + prefetchSize,
            "reportDirectory=" + reportDirectory,
        };
        return options;
    }
}
