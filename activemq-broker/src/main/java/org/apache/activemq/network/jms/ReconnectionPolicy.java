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

package org.apache.activemq.network.jms;

/**
 * A policy object that defines how a {@link JmsConnector} deals with
 * reconnection of the local and foreign connections.
 *
 * @org.apache.xbean.XBean element="reconnectionPolicy"
 */
public class ReconnectionPolicy {

    public static final int INFINITE = -1;

    private int maxSendRetries = 10;
    private long sendRetryDelay = 1000L;

    private int maxReconnectAttempts = INFINITE;
    private int maxInitialConnectAttempts = INFINITE;
    private long maximumReconnectDelay = 30000;
    private long initialReconnectDelay = 1000L;
    private boolean useExponentialBackOff = false;
    private double backOffMultiplier = 2.0;

    /**
     * Gets the maximum number of a times a Message send should be retried before
     * a JMSExeception is thrown indicating that the operation failed.
     *
     * @return number of send retries that will be performed.
     */
    public int getMaxSendRetries() {
        return maxSendRetries;
    }

    /**
     * Sets the maximum number of a times a Message send should be retried before
     * a JMSExeception is thrown indicating that the operation failed.
     *
     * @param maxSendRetries
     * 			number of send retries that will be performed.
     */
    public void setMaxSendRetries(int maxSendRetries) {
        this.maxSendRetries = maxSendRetries;
    }

    /**
     * Get the amount of time the DestionationBridge will wait between attempts
     * to forward a message.
     *
     * @return time in milliseconds to wait between send attempts.
     */
    public long getSendRetryDelay() {
        return this.sendRetryDelay;
    }

    /**
     * Set the amount of time the DestionationBridge will wait between attempts
     * to forward a message.  The default policy limits the minimum time between
     * send attempt to one second.
     *
     * @param sendRetryDelay
     * 		Time in milliseconds to wait before attempting another send.
     */
    public void setSendRetyDelay(long sendRetryDelay) {
        if (sendRetryDelay < 1000L) {
            this.sendRetryDelay = 1000L;
        }

        this.sendRetryDelay = sendRetryDelay;
    }

    /**
     * Gets the number of time that {@link JmsConnector} will attempt to connect
     * or reconnect before giving up.  By default the policy sets this value to
     * a negative value meaning try forever.
     *
     * @return the number of attempts to connect before giving up.
     */
    public int getMaxReconnectAttempts() {
        return maxReconnectAttempts;
    }

    /**
     * Sets the number of time that {@link JmsConnector} will attempt to connect
     * or reconnect before giving up.  By default the policy sets this value to
     * a negative value meaning try forever, set to a positive value to retry a
     * fixed number of times, or zero to never try and reconnect.
     *
     * @param maxReconnectAttempts
     */
    public void setMaxReconnectAttempts(int maxReconnectAttempts) {
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    /**
     * Gets the maximum number of times that the {@link JmsConnector} will try
     * to connect on startup to before it marks itself as failed and does not
     * try any further connections.
     *
     * @returns the max number of times a connection attempt is made before failing.
     */
    public int getMaxInitialConnectAttempts() {
        return this.maxInitialConnectAttempts;
    }

    /**
     * Sets the maximum number of times that the {@link JmsConnector} will try
     * to connect on startup to before it marks itself as failed and does not
     * try any further connections.
     *
     * @param maxAttempts
     * 		The max number of times a connection attempt is made before failing.
     */
    public void setMaxInitialConnectAttempts(int maxAttempts) {
        this.maxInitialConnectAttempts = maxAttempts;
    }

    /**
     * Gets the maximum delay that is inserted between each attempt to connect
     * before another attempt is made.  The default setting for this value is
     * 30 seconds.
     *
     * @return the max delay between connection attempts in milliseconds.
     */
    public long getMaximumReconnectDelay() {
        return maximumReconnectDelay;
    }

    /**
     * Sets the maximum delay that is inserted between each attempt to connect
     * before another attempt is made.
     *
     * @param maximumReconnectDelay
     * 		The maximum delay between connection attempts in milliseconds.
     */
    public void setMaximumReconnectDelay(long maximumReconnectDelay) {
        this.maximumReconnectDelay = maximumReconnectDelay;
    }

    /**
     * Gets the initial delay value used before a reconnection attempt is made.  If the
     * use exponential back-off value is set to false then this will be the fixed time
     * between connection attempts.  By default this value is set to one second.
     *
     * @return time in milliseconds that will be used between connection retries.
     */
    public long getInitialReconnectDelay() {
        return initialReconnectDelay;
    }

    /**
     * Gets the initial delay value used before a reconnection attempt is made.  If the
     * use exponential back-off value is set to false then this will be the fixed time
     * between connection attempts.  By default this value is set to one second.

     * @param initialReconnectDelay
     * 		Time in milliseconds to wait before the first reconnection attempt.
     */
    public void setInitialReconnectDelay(long initialReconnectDelay) {
        this.initialReconnectDelay = initialReconnectDelay;
    }

    /**
     * Gets whether the policy uses the set back-off multiplier to grow the time between
     * connection attempts.
     *
     * @return true if the policy will grow the time between connection attempts.
     */
    public boolean isUseExponentialBackOff() {
        return useExponentialBackOff;
    }

    /**
     * Sets whether the policy uses the set back-off multiplier to grow the time between
     * connection attempts.
     *
     * @param useExponentialBackOff
     */
    public void setUseExponentialBackOff(boolean useExponentialBackOff) {
        this.useExponentialBackOff = useExponentialBackOff;
    }

    /**
     * Gets the multiplier used to grow the delay between connection attempts from the initial
     * time to the max set time.  By default this value is set to 2.0.
     *
     * @return the currently configured connection delay multiplier.
     */
    public double getBackOffMultiplier() {
        return backOffMultiplier;
    }

    /**
     * Gets the multiplier used to grow the delay between connection attempts from the initial
     * time to the max set time.  By default this value is set to 2.0.
     *
     * @param backOffMultiplier
     * 		The multiplier value used to grow the reconnection delay.
     */
    public void setBackOffMultiplier(double backOffMultiplier) {
        this.backOffMultiplier = backOffMultiplier;
    }

    /**
     * Returns the next computed delay value that the connection controller should use to
     * wait before attempting another connection for the {@link JmsConnector}.
     *
     * @param attempt
     * 		The current connection attempt.
     *
     * @return the next delay amount in milliseconds.
     */
    public long getNextDelay(int attempt) {

        if (attempt == 0) {
            return 0;
        }

        long nextDelay = initialReconnectDelay;

        if (useExponentialBackOff) {
            nextDelay = Math.max(initialReconnectDelay, nextDelay * (long)((attempt - 1) * backOffMultiplier));
        }

        if (maximumReconnectDelay > 0 && nextDelay > maximumReconnectDelay) {
            nextDelay = maximumReconnectDelay;
        }

        return nextDelay;
    }
}
