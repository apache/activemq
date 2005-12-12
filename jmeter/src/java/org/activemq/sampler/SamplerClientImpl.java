/**
 *
 * Copyright 2004 Protique Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.sampler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;

import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

/**
 *
 */
public class SamplerClientImpl implements SamplerClient {

    private static Logger log = LoggingManager.getLoggerForClass();
    private byte eolByte = (byte) JMeterUtils.getPropDefault("tcp.prod.eolByte", 0);

    /**
     * Constructor for the ProducerClientImpl object.
     */
    public SamplerClientImpl() {

        super();
        log.info("Using eolByte=" + eolByte);
    }

    /**
     * Logs "setuptest".
     */
    public void setupTest() {

        log.info("setuptest");
    }

    /**
     * Logs "teardowntest".
     */
    public void teardownTest() {

        log.info("teardowntest");
    }

    /**
     * Writes String object to OutputStream object.
     *
     * @param os - OutputStream object.
     * @param s - String object.
     */
    public void write(OutputStream os, String s) {

        try {
            os.write(s.getBytes());
            os.flush();
        } catch (IOException e) {
            log.debug("Write error", e);
        }

        log.debug("Wrote: " + s);

        return;
    }

    /**
     * Converts InputStream object to String.
     *
     * @param is - InputSream object.
     * @return contains information from InputStream object parameter.
     */
    public String read(InputStream is) {

        byte[] buffer = new byte[4096];
        ByteArrayOutputStream w = new ByteArrayOutputStream();
        int x = 0;
        try {
            while ((x = is.read(buffer)) > -1) {
                w.write(buffer, 0, x);
                if ((eolByte != 0) && (buffer[x - 1] == eolByte)) {
                    break;
                }
            }

        } catch (InterruptedIOException e) {
            // drop out to handle buffer
        } catch (IOException e) {
            log.warn("Read error:" + e);
            return "";
        }

        // do we need to close byte array (or flush it?)
        log.debug("Read: " + w.size() + "\n" + w.toString());

        return w.toString();
    }

    /**
     * Non-functional implementation of the write method of
     * the implementated class.
     *
     * @param os - OutputStream object.
     * @param is - InputStream.
     */
    public void write(OutputStream os, InputStream is) {

        // TODO Auto-generated method stub
        return;
    }

    /**
     * @return Returns the eolByte instance variable.
     */
    public byte getEolByte() {

        return eolByte;
    }

    /**
     * Sets value to eolByte instance variable.
     *
     * @param eolByte - The eolByte to set..
     */
    public void setEolByte(byte eolByte) {

        this.eolByte = eolByte;
    }
}
