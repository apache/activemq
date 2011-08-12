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

public interface Stomp {
    String NULL = "\u0000";
    String NEWLINE = "\n";

    byte BREAK = '\n';
    byte COLON = ':';
    byte ESCAPE = '\\';
    byte[] ESCAPE_ESCAPE_SEQ = { 92, 92 };
    byte[] COLON_ESCAPE_SEQ = { 92, 99 };
    byte[] NEWLINE_ESCAPE_SEQ = { 92, 110 };

    String COMMA = ",";
    String V1_0 = "1.0";
    String V1_1 = "1.1";
    String DEFAULT_HEART_BEAT = "0,0";
    String DEFAULT_VERSION = "1.0";
    String EMPTY = "";

    String[] SUPPORTED_PROTOCOL_VERSIONS = {"1.1", "1.0"};

    String TEXT_PLAIN = "text/plain";
    String TRUE = "true";
    String FALSE = "false";
    String END = "end";

    public static interface Commands {
        String STOMP = "STOMP";
        String CONNECT = "CONNECT";
        String SEND = "SEND";
        String DISCONNECT = "DISCONNECT";
        String SUBSCRIBE = "SUB";
        String UNSUBSCRIBE = "UNSUB";

        String BEGIN_TRANSACTION = "BEGIN";
        String COMMIT_TRANSACTION = "COMMIT";
        String ABORT_TRANSACTION = "ABORT";
        String BEGIN = "BEGIN";
        String COMMIT = "COMMIT";
        String ABORT = "ABORT";
        String ACK = "ACK";
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
        String CONTENT_TYPE = "content-type";
        String TRANSFORMATION = "transformation";
        String TRANSFORMATION_ERROR = "transformation-error";

        /**
         * This header is used to instruct ActiveMQ to construct the message
         * based with a specific type.
         */
        String AMQ_MESSAGE_TYPE = "amq-msg-type";

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
            String PERSISTENT = "persistent";
        }

        public interface Message {
            String MESSAGE_ID = "message-id";
            String DESTINATION = "destination";
            String CORRELATION_ID = "correlation-id";
            String EXPIRATION_TIME = "expires";
            String REPLY_TO = "reply-to";
            String PRORITY = "priority";
            String REDELIVERED = "redelivered";
            String TIMESTAMP = "timestamp";
            String TYPE = "type";
            String SUBSCRIPTION = "subscription";
            String BROWSER = "browser";
            String USERID = "JMSXUserID";
            String ORIGINAL_DESTINATION = "original-destination";
        }

        public interface Subscribe {
            String DESTINATION = "destination";
            String ACK_MODE = "ack";
            String ID = "id";
            String SELECTOR = "selector";
            String BROWSER = "browser";

            public interface AckModeValues {
                String AUTO = "auto";
                String CLIENT = "client";
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
            String REQUEST_ID = "request-id";
            String ACCEPT_VERSION = "accept-version";
            String HOST = "host";
            String HEART_BEAT = "heart-beat";
        }

        public interface Error {
            String MESSAGE = "message";
        }

        public interface Connected {
            String SESSION = "session";
            String RESPONSE_ID = "response-id";
            String SERVER = "server";
            String VERSION = "version";
            String HEART_BEAT = "heart-beat";
        }

        public interface Ack {
            String MESSAGE_ID = "message-id";
            String SUBSCRIPTION = "subscription";
        }
    }

    public enum Transformations {
        JMS_BYTE,
        JMS_XML,
        JMS_JSON,
        JMS_OBJECT_XML,
        JMS_OBJECT_JSON,
        JMS_MAP_XML,
        JMS_MAP_JSON,
        JMS_ADVISORY_XML,
        JMS_ADVISORY_JSON;

        public String toString() {
            return name().replaceAll("_", "-").toLowerCase();
        }

        public static Transformations getValue(String value) {
            return valueOf(value.replaceAll("-", "_").toUpperCase());
        }
    }
}
