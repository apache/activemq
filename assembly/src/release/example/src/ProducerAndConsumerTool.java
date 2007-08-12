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

import java.util.Arrays;
import java.util.HashSet;

import javax.jms.MessageListener;

/**
 * A simple tool for producing and consuming messages
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class ProducerAndConsumerTool extends ConsumerTool implements MessageListener {

    public static void main(String[] args) {

        ConsumerTool consumerTool = new ConsumerTool();
        String[] unknown = CommandLineSupport.setOptions(consumerTool, args);
        HashSet<String> set1 = new HashSet<String>(Arrays.asList(unknown));

        ProducerTool producerTool = new ProducerTool();
        unknown = CommandLineSupport.setOptions(producerTool, args);
        HashSet<String> set2 = new HashSet<String>(Arrays.asList(unknown));

        set1.retainAll(set2);
        if (set1.size() > 0) {
            System.out.println("Unknown options: " + set1);
            System.exit(-1);
        }

        consumerTool.run();
        producerTool.run();

    }

}
