package org.apache.activemq.maven;

import org.apache.activemq.tool.JmsProducerClient;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

import javax.jms.JMSException;

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
     * @parameter expression="${url}" default-value="tcp://localhost:61616"
     * @required
     */
    private String url;
    /**
     * @parameter expression="${topic}" default-value="true"
     * @required
     */
    private String topic;
    /**
     * @parameter expression="${subject}" default-value="FOO.BAR"
     * @required
     */
    private String subject;
    /**
     * @parameter expression="${durable}" default-value="false"
     * @required
     */
    private String durable;
    /**
     * @parameter expression="${messageCount}" default-value="10"
     * @required
     */
    private String messageCount;
    /**
     * @parameter expression="${messageSize}" default-value="255"
     * @required
     */
    private String messageSize;

    public void execute()
            throws MojoExecutionException {

        String[] args = {url, topic, subject, durable, messageCount, messageSize};
        try {
            JmsProducerClient.main(args);
        } catch (JMSException e) {
            throw new MojoExecutionException("Error executing Producer: " + e.getMessage());
        }
    }
}
