/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.Response;

import java.io.DataOutput;
import java.io.IOException;

interface ResponseListener {
    /**
     * Return true if you handled this, false otherwise
     */
    boolean onResponse(Response receipt, DataOutput out) throws IOException;
}
