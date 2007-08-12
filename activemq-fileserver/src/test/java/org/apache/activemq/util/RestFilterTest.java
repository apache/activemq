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
package org.apache.activemq.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import junit.framework.TestCase;

import org.mortbay.util.IO;

public class RestFilterTest extends TestCase {

    public RestFilterTest(String s) {
        super(s);
    }

    public void test() throws Exception {
        byte[] fileContents = new byte[] {
            'a', 'b', 'c'
        };
        URL url = new URL("http://localhost:8080/fileserver/repository/file.txt");

        // 1. upload
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        connection.setChunkedStreamingMode(fileContents.length);
        OutputStream os = connection.getOutputStream();
        IO.copy(new ByteArrayInputStream(fileContents), os);
        os.close();
        assertTrue(isSuccessfulCode(connection.getResponseCode()));
        connection.disconnect();

        // 2. download
        connection = (HttpURLConnection)url.openConnection();
        InputStream is = connection.getInputStream();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IO.copy(is, baos);
        is.close();
        assertTrue(isSuccessfulCode(connection.getResponseCode()));
        connection.disconnect();
        assertEquals(fileContents.length, baos.size());

        // 3. remove
        connection = (HttpURLConnection)url.openConnection();
        connection.setRequestMethod("DELETE");
        is = connection.getInputStream();
        is.close();
        assertTrue(isSuccessfulCode(connection.getResponseCode()));
    }

    private boolean isSuccessfulCode(int responseCode) {
        return responseCode >= 200 && responseCode < 300; // 2xx => successful
    }
}
