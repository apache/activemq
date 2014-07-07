package org.apache.activemq.store.kahadb.scheduler;

import java.io.IOException;

public class UnknownStoreVersionException extends IOException {

    private static final long serialVersionUID = -8544753506151157145L;

    private final String token;

    public UnknownStoreVersionException(Throwable cause) {
        super(cause);
        this.token = "";
    }

    public UnknownStoreVersionException(String token) {
        super("Failed to load Store, found unknown store token: " + token);
        this.token = token;
    }

    public String getToken() {
        return this.token;
    }
}
