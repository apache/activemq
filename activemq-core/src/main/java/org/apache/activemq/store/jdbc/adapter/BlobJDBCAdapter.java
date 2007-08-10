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
package org.apache.activemq.store.jdbc.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.jms.JMSException;

import org.apache.activemq.store.jdbc.TransactionContext;
import org.apache.activemq.util.ByteArrayOutputStream;

/**
 * This JDBCAdapter inserts and extracts BLOB data using the getBlob()/setBlob()
 * operations. This is a little more involved since to insert a blob you have
 * to:
 * 
 * 1: insert empty blob. 2: select the blob 3: finally update the blob with data
 * value.
 * 
 * The databases/JDBC drivers that use this adapter are:
 * <ul>
 * <li></li>
 * </ul>
 * 
 * @org.apache.xbean.XBean element="blobJDBCAdapter"
 * 
 * @version $Revision: 1.2 $
 */
public class BlobJDBCAdapter extends DefaultJDBCAdapter {

    public void doAddMessage(Connection c, long seq, String messageID, String destinationName, byte[] data)
        throws SQLException, JMSException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {

            // Add the Blob record.
            s = c.prepareStatement(statements.getAddMessageStatement());
            s.setLong(1, seq);
            s.setString(2, destinationName);
            s.setString(3, messageID);
            s.setString(4, " ");

            if (s.executeUpdate() != 1) {
                throw new JMSException("Failed to broker message: " + messageID + " in container.");
            }
            s.close();

            // Select the blob record so that we can update it.
            s = c.prepareStatement(statements.getFindMessageStatement());
            s.setLong(1, seq);
            rs = s.executeQuery();
            if (!rs.next()) {
                throw new JMSException("Failed to broker message: " + messageID + " in container.");
            }

            // Update the blob
            Blob blob = rs.getBlob(1);
            OutputStream stream = blob.setBinaryStream(data.length);
            stream.write(data);
            stream.close();
            s.close();

            // Update the row with the updated blob
            s = c.prepareStatement(statements.getUpdateMessageStatement());
            s.setBlob(1, blob);
            s.setLong(2, seq);

        } catch (IOException e) {
            throw (SQLException)new SQLException("BLOB could not be updated: " + e).initCause(e);
        } finally {
            try {
                rs.close();
            } catch (Throwable ignore) {
            }
            try {
                s.close();
            } catch (Throwable ignore) {
            }
        }
    }

    public byte[] doGetMessage(TransactionContext c, long seq) throws SQLException {
        PreparedStatement s = null;
        ResultSet rs = null;
        try {

            s = c.getConnection().prepareStatement(statements.getFindMessageStatement());
            s.setLong(1, seq);
            rs = s.executeQuery();

            if (!rs.next()) {
                return null;
            }
            Blob blob = rs.getBlob(1);
            InputStream is = blob.getBinaryStream();

            ByteArrayOutputStream os = new ByteArrayOutputStream((int)blob.length());
            int ch;
            while ((ch = is.read()) >= 0) {
                os.write(ch);
            }
            is.close();
            os.close();

            return os.toByteArray();

        } catch (IOException e) {
            throw (SQLException)new SQLException("BLOB could not be updated: " + e).initCause(e);
        } finally {
            try {
                rs.close();
            } catch (Throwable ignore) {
            }
            try {
                s.close();
            } catch (Throwable ignore) {
            }
        }
    }

}
