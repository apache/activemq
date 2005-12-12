/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

class AsyncHelper {
    public static Object tryUntilNotInterrupted(HelperWithReturn helper) {
        while (true) {
            try {
                return helper.cycle();
            }
            catch (InterruptedException e) { /* */
            }
        }
    }

    static void tryUntilNotInterrupted(final Helper helper) {
        tryUntilNotInterrupted(new HelperWithReturn() {

            public Object cycle() throws InterruptedException {
                helper.cycle();
                return null;
            }
        });
    }

    interface HelperWithReturn {
        Object cycle() throws InterruptedException;
    }

    interface Helper {
        void cycle() throws InterruptedException;
    }
}
