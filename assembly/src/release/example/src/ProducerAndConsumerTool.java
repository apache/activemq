/**
 *
 * Copyright 2004 Protique Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.io.IOException;

/**
 * A simple tool for producing and consuming messages
 *
 * @version $Revision: 1.1.1.1 $
 */
public class ProducerAndConsumerTool extends ConsumerTool implements MessageListener {

    public static void main(String[] args) {
        ProducerAndConsumerTool tool = new ProducerAndConsumerTool();
        if (args.length > 0) {
            tool.url = args[0];
        }
        else {
            tool.url = "vm://localhost";
        }
        if (args.length > 1) {
            tool.topic = args[1].equalsIgnoreCase("true");
        }
        if (args.length > 2) {
            tool.subject = args[2];
        }
        if (args.length > 3) {
            tool.durable = args[3].equalsIgnoreCase("true");
        }
        if (args.length > 4) {
            tool.maxiumMessages = Integer.parseInt(args[4]);
        }
        if (args.length > 5) {
            tool.clientID = args[5];
        }
        tool.run();
    }

    public void run() {
        super.run();

        // now lets publish some messages
        ProducerTool tool = new ProducerTool();
        tool.url = this.url;
        tool.topic = this.topic;
        tool.subject = this.subject;
        tool.durable = this.durable;
        tool.clientID = this.clientID;

        tool.run();
    }


}