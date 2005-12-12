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
