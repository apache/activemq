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
		byte[] fileContents = new byte[] {'a', 'b', 'c'};	
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
