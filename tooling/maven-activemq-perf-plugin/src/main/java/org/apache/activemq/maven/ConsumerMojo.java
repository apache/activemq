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

import org.apache.activemq.tool.JmsConsumerClient;
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
     * @parameter expression="${subject}"  default-value="FOO.BAR"
     * @required
     */
    private String subject;

    /**
     * @parameter expression="${durable}" default-value="false"
     * @required
     */
    private String durable;

    /**
     * @parameter expression="${maximumMessage}" default-value="10"
     * @required
     */
    private String maximumMessage;

    public void execute()
            throws MojoExecutionException {

        String[] args = {url, topic, subject, durable, maximumMessage};
        try {
            JmsConsumerClient.main(args);
        } catch (JMSException e) {
            throw new MojoExecutionException("Error Executing Consumer: " + e.getMessage());
        }
    }
}
