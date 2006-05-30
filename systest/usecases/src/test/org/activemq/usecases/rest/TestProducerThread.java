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
