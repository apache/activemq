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
package org.apache.activemq.broker.virtual;

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleMessageListener implements MessageListener {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleMessageListener.class);

  private CountDownLatch messageReceivedToken;

  private String lastJMSDestination;

  @Override
  public void onMessage(Message message) {
    try {
      Thread.sleep(2000L);
      if (message instanceof TextMessage) {
        LOG.info("Dest:" + message.getJMSDestination());
        lastJMSDestination = message.getJMSDestination().toString();

        Enumeration propertyNames = message.getPropertyNames();
        while (propertyNames.hasMoreElements()) {
          Object object = propertyNames.nextElement();
        }

      }
      messageReceivedToken.countDown();

    }
    catch (JMSException e) {
      LOG.error("Error while listening to a message", message);
    }
    catch (InterruptedException e) {
      LOG.error("Interrupted while listening to a message", message);
    }
  }

  /**
   * @param countDown
   *          the countDown to set
   */
  public void setCountDown(CountDownLatch countDown) {
    this.messageReceivedToken = countDown;
  }

  /**
   * @return the lastJMSDestination
   */
  public String getLastJMSDestination() {
    return lastJMSDestination;
  }

}

