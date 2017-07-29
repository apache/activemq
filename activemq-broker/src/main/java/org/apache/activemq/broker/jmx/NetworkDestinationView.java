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
package org.apache.activemq.broker.jmx;

import org.apache.activemq.management.TimeStatisticImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkDestinationView implements NetworkDestinationViewMBean {
    private static final Logger LOG = LoggerFactory.getLogger(NetworkDestinationView.class);
    private TimeStatisticImpl timeStatistic = new TimeStatisticImpl("networkEnqueue","network messages enqueued");

    private final String name;
    private final NetworkBridgeView networkBridgeView;
    private long lastTime = -1;

    public NetworkDestinationView(NetworkBridgeView networkBridgeView, String name){
       this.networkBridgeView = networkBridgeView;
       this.name=name;
    }
    /**
     * Returns the name of this destination
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Resets the managment counters.
     */
    @Override
    public void resetStats() {
        timeStatistic.reset();
        lastTime = -1;
    }


    @Override
    public long getCount() {
        return timeStatistic.getCount();
    }

    @Override
    public double getRate() {
        return timeStatistic.getAveragePerSecond();
    }

    public void messageSent(){
        long currentTime = System.currentTimeMillis();
        long time = 0;
        if (lastTime < 0){
            time = 0;
            lastTime = currentTime;
        }else{
            time = currentTime-lastTime;
        }
        timeStatistic.addTime(time);
        lastTime=currentTime;
    }

    public long getLastAccessTime(){
        return timeStatistic.getLastSampleTime();
    }

    public void close(){
        networkBridgeView.removeNetworkDestinationView(this);
    }


}
