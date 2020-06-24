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
package org.apache.activemq.transport.stomp;

import java.util.Locale;

public interface Stomp {
    String NULL = "\u0000";
    String NEWLINE = "\n";

//IC see: https://issues.apache.org/jira/browse/AMQ-3449
    byte BREAK = '\n';
    byte COLON = ':';
    byte ESCAPE = '\\';
    byte[] ESCAPE_ESCAPE_SEQ = { 92, 92 };
    byte[] COLON_ESCAPE_SEQ = { 92, 99 };
    byte[] NEWLINE_ESCAPE_SEQ = { 92, 110 };

    String COMMA = ",";
    String V1_0 = "1.0";
    String V1_1 = "1.1";
//IC see: https://issues.apache.org/jira/browse/AMQ-4129
    String V1_2 = "1.2";
    String DEFAULT_HEART_BEAT = "0,0";
    String DEFAULT_VERSION = "1.0";
    String EMPTY = "";

    String[] SUPPORTED_PROTOCOL_VERSIONS = {"1.2", "1.1", "1.0"};

    String TEXT_PLAIN = "text/plain";
    String TRUE = "true";
    String FALSE = "false";
    String END = "end";

    public static interface Commands {
        String STOMP = "STOMP";
        String CONNECT = "CONNECT";
        String SEND = "SEND";
        String DISCONNECT = "DISCONNECT";
//IC see: https://issues.apache.org/jira/browse/AMQ-7012
        String SUBSCRIBE = "SUBSCRIBE";
        String UNSUBSCRIBE = "UNSUBSCRIBE";

        // Preserve legacy incorrect allow shortened names for
        // subscribe and un-subscribe as it has been there for so
        // long that someone has undoubtedly come to expect it.
        String SUBSCRIBE_PREFIX = "SUB";
        String UNSUBSCRIBE_PREFIX = "UNSUB";

        String BEGIN_TRANSACTION = "BEGIN";
        String COMMIT_TRANSACTION = "COMMIT";
        String ABORT_TRANSACTION = "ABORT";
        String BEGIN = "BEGIN";
        String COMMIT = "COMMIT";
        String ABORT = "ABORT";
        String ACK = "ACK";
//IC see: https://issues.apache.org/jira/browse/AMQ-3449
        String NACK = "NACK";
        String KEEPALIVE = "KEEPALIVE";
    }

    public interface Responses {
        String CONNECTED = "CONNECTED";
        String ERROR = "ERROR";
        String MESSAGE = "MESSAGE";
        String RECEIPT = "RECEIPT";
    }

    public interface Headers {
        String SEPERATOR = ":";
        String RECEIPT_REQUESTED = "receipt";
        String TRANSACTION = "transaction";
        String CONTENT_LENGTH = "content-length";
//IC see: https://issues.apache.org/jira/browse/AMQ-3449
        String CONTENT_TYPE = "content-type";
//IC see: https://issues.apache.org/jira/browse/AMQ-943
        String TRANSFORMATION = "transformation";
        String TRANSFORMATION_ERROR = "transformation-error";
//IC see: https://issues.apache.org/jira/browse/AMQ-1567

        /**
         * This header is used to instruct ActiveMQ to construct the message
         * based with a specific type.
         */
        String AMQ_MESSAGE_TYPE = "amq-msg-type";
//IC see: https://issues.apache.org/jira/browse/AMQ-2833

        public interface Response {
            String RECEIPT_ID = "receipt-id";
        }

        public interface Send {
            String DESTINATION = "destination";
            String CORRELATION_ID = "correlation-id";
            String REPLY_TO = "reply-to";
            String EXPIRATION_TIME = "expires";
            String PRIORITY = "priority";
            String TYPE = "type";
//IC see: https://issues.apache.org/jira/browse/AMQ-2817
            String PERSISTENT = "persistent";
        }

        public interface Message {
            String MESSAGE_ID = "message-id";
//IC see: https://issues.apache.org/jira/browse/AMQ-4129
            String ACK_ID = "ack";
            String DESTINATION = "destination";
            String CORRELATION_ID = "correlation-id";
            String EXPIRATION_TIME = "expires";
            String REPLY_TO = "reply-to";
            String PRORITY = "priority";
            String REDELIVERED = "redelivered";
            String TIMESTAMP = "timestamp";
            String TYPE = "type";
            String SUBSCRIPTION = "subscription";
//IC see: https://issues.apache.org/jira/browse/AMQ-3449
            String BROWSER = "browser";
//IC see: https://issues.apache.org/jira/browse/AMQ-2490
            String USERID = "JMSXUserID";
//IC see: https://issues.apache.org/jira/browse/AMQ-3146
            String ORIGINAL_DESTINATION = "original-destination";
//IC see: https://issues.apache.org/jira/browse/AMQ-3475
            String PERSISTENT = "persistent";
        }

        public interface Subscribe {
            String DESTINATION = "destination";
            String ACK_MODE = "ack";
            String ID = "id";
            String SELECTOR = "selector";
            String BROWSER = "browser";
//IC see: https://issues.apache.org/jira/browse/AMQ-3449

            public interface AckModeValues {
                String AUTO = "auto";
                String CLIENT = "client";
//IC see: https://issues.apache.org/jira/browse/AMQ-1874
                String INDIVIDUAL = "client-individual";
            }
        }

        public interface Unsubscribe {
            String DESTINATION = "destination";
            String ID = "id";
        }

        public interface Connect {
            String LOGIN = "login";
            String PASSCODE = "passcode";
            String CLIENT_ID = "client-id";
//IC see: https://issues.apache.org/jira/browse/AMQ-748
            String REQUEST_ID = "request-id";
//IC see: https://issues.apache.org/jira/browse/AMQ-3449
            String ACCEPT_VERSION = "accept-version";
            String HOST = "host";
            String HEART_BEAT = "heart-beat";
        }

        public interface Error {
            String MESSAGE = "message";
        }

        public interface Connected {
            String SESSION = "session";
//IC see: https://issues.apache.org/jira/browse/AMQ-748
            String RESPONSE_ID = "response-id";
//IC see: https://issues.apache.org/jira/browse/AMQ-3449
            String SERVER = "server";
            String VERSION = "version";
            String HEART_BEAT = "heart-beat";
        }

        public interface Ack {
            String MESSAGE_ID = "message-id";
            String SUBSCRIPTION = "subscription";
//IC see: https://issues.apache.org/jira/browse/AMQ-4129
            String ACK_ID = "id";
        }
    }

    public enum Transformations {
//IC see: https://issues.apache.org/jira/browse/AMQ-2098
        JMS_BYTE,
        JMS_XML,
        JMS_JSON,
        JMS_OBJECT_XML,
        JMS_OBJECT_JSON,
        JMS_MAP_XML,
        JMS_MAP_JSON,
        JMS_ADVISORY_XML,
        JMS_ADVISORY_JSON;

        @Override
        public String toString() {
//IC see: https://issues.apache.org/jira/browse/AMQ-4012
            return name().replaceAll("_", "-").toLowerCase(Locale.ENGLISH);
        }

        public boolean equals(String value) {
//IC see: https://issues.apache.org/jira/browse/AMQ-5220
            return toString().equals(value);
        }

        public static Transformations getValue(String value) {
            return valueOf(value.replaceAll("-", "_").toUpperCase(Locale.ENGLISH));
        }
    }
}
