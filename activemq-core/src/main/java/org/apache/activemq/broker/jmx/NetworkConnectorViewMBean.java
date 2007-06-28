/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.jmx;

import org.apache.activemq.Service;

public interface NetworkConnectorViewMBean extends Service {

      public String getName();
      public int getNetworkTTL();
      public int getPrefetchSize();
      public String getUserName();
      public boolean isBridgeTempDestinations();
      public boolean isConduitSubscriptions();
      public boolean isDecreaseNetworkConsumerPriority();
      public boolean isDispatchAsync();
      public boolean isDynamicOnly();
      public void setBridgeTempDestinations(boolean bridgeTempDestinations);
      public void setConduitSubscriptions(boolean conduitSubscriptions);
      public void setDispatchAsync(boolean dispatchAsync);
      public void setDynamicOnly(boolean dynamicOnly);
      public void setNetworkTTL(int networkTTL);
      public void setPassword(String password);
      public void setPrefetchSize(int prefetchSize);
      public void setUserName(String userName);
      public String getPassword();
      public void setDecreaseNetworkConsumerPriority(boolean decreaseNetworkConsumerPriority);

}
