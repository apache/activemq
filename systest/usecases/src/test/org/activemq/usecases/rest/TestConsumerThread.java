/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.usecases.rest;

import java.io.*;
import java.net.*;

class TestConsumerThread extends Thread {
    private RESTLoadTest c;
    public int success;
    private int messagecount;

    TestConsumerThread(RESTLoadTest p, int count) {
        c = p;
        success = 0;
        c.counter++;
        messagecount = count;
    }

    private int performGet(String urlString) {
        try {
            URL url;
            HttpURLConnection urlConn;

            DataOutputStream printout;
            DataInputStream input;

            // URL of CGI-Bin script.
            url = new URL(urlString);

            // URL connection channel.
            urlConn = (HttpURLConnection) url.openConnection();
            urlConn.setDoInput(true);
            urlConn.setDoOutput(true);
            urlConn.setUseCaches(false);

            // Get response data.
            input = new DataInputStream(urlConn.getInputStream());

            String str;
            while (null != ((str = input.readLine()))) {
                System.out.println("CONSUME:" + str);
            }

            input.close();
            success++;
            return 0;

        }
        catch (MalformedURLException me) {
            System.err.println("MalformedURLException: " + me);
            return 1;
        }
        catch (IOException ioe) {
            System.err.println("IOException: " + ioe.getMessage());
            return 1;
        }
    }

    public void run() {
        for (int i = 0; i < messagecount; i++) {
            int e = performGet("http://127.0.0.1:8080/jms/FOO/BAR?id=1234&readTimeout=60000");
            if (e == 1) {
                break;
            }
        }
        c.counter--;

    }
}
