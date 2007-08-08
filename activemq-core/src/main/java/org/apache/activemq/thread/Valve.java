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
package org.apache.activemq.thread;

/**
 * A Valve is a synchronization object used enable or disable the "flow" of
 * concurrent processing.
 * 
 * @version $Revision: 1.2 $
 */
final public class Valve {

    private final Object mutex = new Object();
    private boolean on;
    private int turningOff = 0;
    private int usage = 0;

    public Valve(boolean on) {
        this.on = on;
    }

    /**
     * Turns the valve on. This method blocks until the valve is off.
     * 
     * @throws InterruptedException
     */
    public void turnOn() throws InterruptedException {
        synchronized (mutex) {
            while (on) {
                mutex.wait();
            }
            on = true;
            mutex.notifyAll();
        }
    }

    boolean isOn() {
        synchronized (mutex) {
            return on;
        }
    }

    /**
     * Turns the valve off. This method blocks until the valve is on and the
     * valve is not in use.
     * 
     * @throws InterruptedException
     */
    public void turnOff() throws InterruptedException {
        synchronized (mutex) {
            try {
                ++turningOff;
                while (usage > 0 || !on) {
                    mutex.wait();
                }
                on = false;
                mutex.notifyAll();
            } finally {
                --turningOff;
            }
        }
    }

    /**
     * Increments the use counter of the valve. This method blocks if the valve
     * is off, or is being turned off.
     * 
     * @throws InterruptedException
     */
    public void increment() throws InterruptedException {
        synchronized (mutex) {
            // Do we have to wait for the value to be on?
            while (turningOff > 0 || !on) {
                mutex.wait();
            }
            usage++;
        }
    }

    /**
     * Decrements the use counter of the valve.
     */
    public void decrement() {
        synchronized (mutex) {
            usage--;
            if (turningOff > 0 && usage < 1) {
                mutex.notifyAll();
            }
        }
    }

}
