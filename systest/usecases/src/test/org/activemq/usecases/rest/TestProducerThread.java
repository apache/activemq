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

import java.net.*;
import java.io.*;

class TestProducerThread extends Thread {
    RESTLoadTest c;
    public int success;
    private int messagecount;

    TestProducerThread(RESTLoadTest p, int count) {
        c = p;
        success = 0;
        c.counter++;
        messagecount = count;

    }

    private int performPost(String urlString, String id, String mesg) {
        try {
            URL url;
            HttpURLConnection urlConn;

            DataOutputStream printout;
            DataInputStream input;

            // URL of CGI-Bin script.
            url = new URL(urlString);

            // URL connection channel.
            urlConn = (HttpURLConnection) url.openConnection();
            urlConn.setRequestMethod("POST");
            urlConn.setDoInput(true);
            urlConn.setDoOutput(true);
            urlConn.setUseCaches(false);
            urlConn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            String content = "id=" + URLEncoder.encode(id) +
                    "&body=" + URLEncoder.encode(mesg);
            urlConn.setRequestProperty("Content-length", Integer.toString(content.length()));

            printout = new DataOutputStream(urlConn.getOutputStream());
            printout.writeBytes(content);
            printout.flush();
            printout.close();

            // Get response data.
            input = new DataInputStream(urlConn.getInputStream());

            String str;
            while (null != ((str = input.readLine()))) {
                System.out.println("PRODUCE:" + str);
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
            int e = performPost("http://127.0.0.1:8080/jms/FOO/BAR", "1234", "Test " + i);
            if (e == 1) {
                break;
            }
        }
        c.counter--;
    }
}
