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
package org.apache.activemq.maven;

import org.apache.activemq.tool.JMSMemtest;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Goal which does a memory usage test to check for any memory leak
 *
 * @goal memtest
 * @phase process-sources
 */
public class MemtestMojo extends AbstractMojo {

    /**
     * @parameter property="url"
     */
    private String url;

    /**
     * @parameter property="topic" default-value="true"
     * @required
     */
    private String topic;

    /**
     * @parameter property="connectionCheckpointSize" default-value="-1"
     * @required
     */
    private String connectionCheckpointSize;

    /**
     * @parameter property="durable" default-value="false"
     * @required
     */
    private String durable;

    /**
     * @parameter property="producerCount" default-value="1"
     * @required
     */
    private String producerCount;

    /**
     * @parameter property="prefetchSize" default-value="-1"
     * @required
     */
    private String prefetchSize;

    /**
     * @parameter property="consumerCount" default-value="1"
     * @required
     */
    private String consumerCount;

    /**
     * @parameter property="messageCount" default-value="100000"
     * @required
     */
    private String messageCount;

    /**
     * @parameter property="messageSize" default-value="10240"
     * @required
     */
    private String messageSize;

    /**
     * @parameter property="checkpointInterval" default-value="2"
     * @required
     */
    private String checkpointInterval;

    /**
     * @parameter property="destinationName" default-value="FOO.BAR"
     * @required
     */
    private String destinationName;

    /**
     * @parameter property="$reportName"
     *            default-value="activemq-memory-usage-report"
     * @required
     */
    private String reportName;

    /**
     * @parameter property="reportDirectory"
     *            default-value="${project.build.directory}/test-memtest"
     * @required
     */
    private String reportDirectory;

    @Override
    public void execute() throws MojoExecutionException {

        JMSMemtest.main(createArgument());
    }

    public String[] createArgument() {
        String[] options = {
            "url=" + url, "topic=" + topic, "durable=" + durable, "connectionCheckpointSize=" + connectionCheckpointSize, "producerCount=" + producerCount, "consumerCount=" + consumerCount,
            "messageCount=" + messageCount, "messageSize=" + messageSize, "checkpointInterval=" + checkpointInterval, "destinationName=" + destinationName, "reportName=" + reportName,
            "prefetchSize=" + prefetchSize, "reportDirectory=" + reportDirectory,
        };
        return options;
    }
}
