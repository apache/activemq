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
package org.apache.activemq.benchmark;

/**
 * @author James Strachan
 * 
 */
public class ProducerConsumer extends Producer {

    private Consumer consumer = new Consumer();

    public ProducerConsumer() {
    }

    public static void main(String[] args) {
        ProducerConsumer tool = new ProducerConsumer();
        if (args.length > 0) {
            tool.setUrl(args[0]);
        }
        if (args.length > 1) {
            tool.setTopic(parseBoolean(args[1]));
        }
        if (args.length > 2) {
            tool.setSubject(args[2]);
        }
        if (args.length > 3) {
            tool.setDurable(Boolean.getBoolean(args[3]));
        }
        if (args.length > 4) {
            tool.setConnectionCount(Integer.parseInt(args[4]));
        }
        try {
            tool.run();
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    public void run() throws Exception {
        consumer.start();
        consumer.subscribe();
        start();
        publish();
    }

    public void setTopic(boolean topic) {
        super.setTopic(topic);
        consumer.setTopic(topic);
    }

    public void setSubject(String subject) {
        super.setSubject(subject);
        consumer.setSubject(subject);
    }

    public void setUrl(String url) {
        super.setUrl(url);
        consumer.setUrl(url);
    }

    protected boolean useTimerLoop() {
        return false;
    }
}
