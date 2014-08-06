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
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.util.IntrospectionSupport;

/**
 * Configuration options for a messageConsumer used to control how messages are re-delivered when they
 * are rolled back.
 * May be used server side on a per destination basis via the Broker RedeliveryPlugin
 *
 * @org.apache.xbean.XBean element="redeliveryPolicy"
 *
 */
public class RedeliveryPolicy extends DestinationMapEntry implements Cloneable, Serializable {

    public static final int NO_MAXIMUM_REDELIVERIES = -1;
    public static final int DEFAULT_MAXIMUM_REDELIVERIES = 6;

    private static Random randomNumberGenerator;

    // +/-15% for a 30% spread -cgs
    protected double collisionAvoidanceFactor = 0.15d;
    protected int maximumRedeliveries = DEFAULT_MAXIMUM_REDELIVERIES;
    protected long maximumRedeliveryDelay = -1;
    protected long initialRedeliveryDelay = 1000L;
    protected boolean useCollisionAvoidance;
    protected boolean useExponentialBackOff;
    protected double backOffMultiplier = 5.0;
    protected long redeliveryDelay = initialRedeliveryDelay;

    public RedeliveryPolicy() {
    }

    public RedeliveryPolicy copy() {
        try {
            return (RedeliveryPolicy)clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Could not clone: " + e, e);
        }
    }

    public double getBackOffMultiplier() {
        return backOffMultiplier;
    }

    public void setBackOffMultiplier(double backOffMultiplier) {
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

    public long getMaximumRedeliveryDelay() {
        return maximumRedeliveryDelay;
    }

    public void setMaximumRedeliveryDelay(long maximumRedeliveryDelay) {
        this.maximumRedeliveryDelay = maximumRedeliveryDelay;
    }

    public int getMaximumRedeliveries() {
        return maximumRedeliveries;
    }

    public void setMaximumRedeliveries(int maximumRedeliveries) {
        this.maximumRedeliveries = maximumRedeliveries;
    }

    public long getNextRedeliveryDelay(long previousDelay) {
        long nextDelay = redeliveryDelay;

        if (previousDelay > 0 && useExponentialBackOff && backOffMultiplier > 1) {
            nextDelay = (long) (previousDelay * backOffMultiplier);
            if(maximumRedeliveryDelay != -1 && nextDelay > maximumRedeliveryDelay) {
                // in case the user made max redelivery delay less than redelivery delay for some reason.
                nextDelay = Math.max(maximumRedeliveryDelay, redeliveryDelay);
            }
        }

        if (useCollisionAvoidance) {
            /*
             * First random determines +/-, second random determines how far to
             * go in that direction. -cgs
             */
            Random random = getRandomNumberGenerator();
            double variance = (random.nextBoolean() ? collisionAvoidanceFactor : -collisionAvoidanceFactor) * random.nextDouble();
            nextDelay += nextDelay * variance;
        }

        return nextDelay;
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

    public void setRedeliveryDelay(long redeliveryDelay) {
        this.redeliveryDelay = redeliveryDelay;
    }

    public long getRedeliveryDelay() {
        return redeliveryDelay;
    }

    @Override
    public String toString() {
        return IntrospectionSupport.toString(this, DestinationMapEntry.class, null);
    }
}
