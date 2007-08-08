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
package org.apache.activemq;

import java.io.Serializable;
import java.util.Random;

/**
 * Configuration options used to control how messages are re-delivered when they
 * are rolled back.
 * 
 * @org.apache.xbean.XBean element="redeliveryPolicy"
 * @version $Revision: 1.11 $
 */
public class RedeliveryPolicy implements Cloneable, Serializable {

    public static final int NO_MAXIMUM_REDELIVERIES = -1;
    private static Random randomNumberGenerator;

    // +/-15% for a 30% spread -cgs
    private double collisionAvoidanceFactor = 0.15d;
    private int maximumRedeliveries = 6;
    private long initialRedeliveryDelay = 1000L;
    private boolean useCollisionAvoidance;
    private boolean useExponentialBackOff;
    private short backOffMultiplier = 5;

    public RedeliveryPolicy() {
    }

    public RedeliveryPolicy copy() {
        try {
            return (RedeliveryPolicy)clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Could not clone: " + e, e);
        }
    }

    public short getBackOffMultiplier() {
        return backOffMultiplier;
    }

    public void setBackOffMultiplier(short backOffMultiplier) {
        this.backOffMultiplier = backOffMultiplier;
    }

    public short getCollisionAvoidancePercent() {
        return (short)Math.round(collisionAvoidanceFactor * 100);
    }

    public void setCollisionAvoidancePercent(short collisionAvoidancePercent) {
        this.collisionAvoidanceFactor = collisionAvoidancePercent * 0.01d;
    }

    public long getInitialRedeliveryDelay() {
        return initialRedeliveryDelay;
    }

    public void setInitialRedeliveryDelay(long initialRedeliveryDelay) {
        this.initialRedeliveryDelay = initialRedeliveryDelay;
    }

    public int getMaximumRedeliveries() {
        return maximumRedeliveries;
    }

    public void setMaximumRedeliveries(int maximumRedeliveries) {
        this.maximumRedeliveries = maximumRedeliveries;
    }

    public long getRedeliveryDelay(long previousDelay) {
        long redeliveryDelay;

        if (previousDelay == 0) {
            redeliveryDelay = initialRedeliveryDelay;
        } else if (useExponentialBackOff && backOffMultiplier > 1) {
            redeliveryDelay = previousDelay * backOffMultiplier;
        } else {
            redeliveryDelay = previousDelay;
        }

        if (useCollisionAvoidance) {
            /*
             * First random determines +/-, second random determines how far to
             * go in that direction. -cgs
             */
            Random random = getRandomNumberGenerator();
            double variance = (random.nextBoolean() ? collisionAvoidanceFactor : -collisionAvoidanceFactor) * random.nextDouble();
            redeliveryDelay += redeliveryDelay * variance;
        }

        return redeliveryDelay;
    }

    public boolean isUseCollisionAvoidance() {
        return useCollisionAvoidance;
    }

    public void setUseCollisionAvoidance(boolean useCollisionAvoidance) {
        this.useCollisionAvoidance = useCollisionAvoidance;
    }

    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }

    protected static synchronized Random getRandomNumberGenerator() {
        if (randomNumberGenerator == null) {
            randomNumberGenerator = new Random();
        }
        return randomNumberGenerator;
    }

}
