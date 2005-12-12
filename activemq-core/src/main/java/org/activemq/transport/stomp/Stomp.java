/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

public interface Stomp {
    String NULL = "\u0000";
    String NEWLINE = "\n";

    public static interface Commands {
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

        public interface Response {
            String RECEIPT_ID = "receipt-id";
        }

        public interface Send {
            String DESTINATION = "destination";
            String CORRELATION_ID = "correlation-id";
            String REPLY_TO = "reply-to";
            String EXPIRATION_TIME = "expires";
            String PRORITY = "priority";
            String TYPE = "type";
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
        }

        public interface Subscribe {
            String DESTINATION = "destination";
            String ACK_MODE = "ack";
            String ID = "id";

            public interface AckModeValues {
                String AUTO = "auto";
                String CLIENT = "client";
            }
        }

        public interface Unsubscribe {
            String DESTINATION = "destination";
        }

        public interface Connect {
            String LOGIN = "login";
            String PASSCODE = "passcode";
        }

        public interface Error {
            String MESSAGE = "message";
        }

        public interface Connected {
            String SESSION = "session";
        }

        public interface Ack {
            String MESSAGE_ID = "message-id";
        }
    }
}
