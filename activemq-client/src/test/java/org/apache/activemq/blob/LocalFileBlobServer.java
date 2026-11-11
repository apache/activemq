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
package org.apache.activemq.blob;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * A minimal blob server over a local folder, for exercising the HTTP blob
 * transfer strategies ({@link DefaultBlobUploadStrategy} /
 * {@link DefaultBlobDownloadStrategy}) in tests without reaching the network.
 *
 * <p>Backed by the JDK {@link HttpServer} on an ephemeral localhost port, it
 * serves {@code PUT}/{@code GET}/{@code DELETE} under a context path
 * ({@code /uploads/} by default): the request path after the context is the
 * blob key, stored as a flat file in the folder (key characters outside
 * {@code [A-Za-z0-9._-]} are mapped to {@code _}, so keys containing path
 * separators or colons round-trip consistently). {@code DELETE} is idempotent.
 */
public class LocalFileBlobServer {

    private static final String CONTEXT = "/uploads/";

    private final Path root;
    private HttpServer server;

    public LocalFileBlobServer(Path root) throws IOException {
        this.root = root;
        Files.createDirectories(root);
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext(CONTEXT, this::handle);
        server.start();
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
            server = null;
        }
    }

    /** The upload URL to configure on a {@link BlobTransferPolicy}. */
    public String getUploadUrl() {
        return "http://localhost:" + server.getAddress().getPort() + CONTEXT;
    }

    /** Pre-seed a blob at an arbitrary key (used to simulate server-chosen layouts). */
    public void put(String key, byte[] data) throws IOException {
        Files.write(fileFor(key), data);
    }

    public boolean exists(String key) {
        return Files.isRegularFile(fileFor(key));
    }

    private Path fileFor(String key) {
        return root.resolve(key.replaceAll("[^A-Za-z0-9._-]", "_"));
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            var key = exchange.getRequestURI().getPath().substring(CONTEXT.length());
            switch (exchange.getRequestMethod()) {
                case "PUT":
                    try (var in = exchange.getRequestBody()) {
                        Files.write(fileFor(key), in.readAllBytes());
                    }
                    exchange.sendResponseHeaders(204, -1);
                    break;
                case "GET":
                    var file = fileFor(key);
                    if (!Files.isRegularFile(file)) {
                        exchange.sendResponseHeaders(404, -1);
                        break;
                    }
                    var body = Files.readAllBytes(file);
                    exchange.sendResponseHeaders(200, body.length);
                    try (var out = exchange.getResponseBody()) {
                        out.write(body);
                    }
                    break;
                case "DELETE":
                    Files.deleteIfExists(fileFor(key));
                    exchange.sendResponseHeaders(204, -1);
                    break;
                default:
                    exchange.sendResponseHeaders(405, -1);
            }
        } finally {
            exchange.close();
        }
    }
}
