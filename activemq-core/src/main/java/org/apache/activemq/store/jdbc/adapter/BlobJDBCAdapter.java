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
import javax.sql.rowset.serial.SerialBlob;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.MessageId;
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
 * 
 */
public class BlobJDBCAdapter extends DefaultJDBCAdapter {

    @Override
    public void doAddMessage(TransactionContext c, long sequence, MessageId messageID, ActiveMQDestination destination, byte[] data,
            long expiration, byte priority) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        cleanupExclusiveLock.readLock().lock();
        try {
            // Add the Blob record.
            s = c.getConnection().prepareStatement(statements.getAddMessageStatement());
            s.setLong(1, sequence);
            s.setString(2, messageID.getProducerId().toString());
            s.setLong(3, messageID.getProducerSequenceId());
            s.setString(4, destination.getQualifiedName());
            s.setLong(5, expiration);
            s.setLong(6, priority);

            if (s.executeUpdate() != 1) {
                throw new IOException("Failed to add broker message: " + messageID + " in container.");
            }
            s.close();

            // Select the blob record so that we can update it.
            s = c.getConnection().prepareStatement(statements.getFindMessageByIdStatement(),
            		ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            s.setLong(1, sequence);
            rs = s.executeQuery();
            if (!rs.next()) {
                throw new IOException("Failed select blob for message: " + messageID + " in container.");
            }

            // Update the blob
            Blob blob = rs.getBlob(1);
            blob.truncate(0);
            blob.setBytes(1, data);
            rs.updateBlob(1, blob);
            rs.updateRow();             // Update the row with the updated blob

        } finally {
            cleanupExclusiveLock.readLock().unlock();
            close(rs);
            close(s);
        }
    }

    @Override
    public byte[] doGetMessage(TransactionContext c, MessageId id) throws SQLException, IOException {
        PreparedStatement s = null;
        ResultSet rs = null;
        cleanupExclusiveLock.readLock().lock();
        try {

            s = c.getConnection().prepareStatement(statements.getFindMessageStatement());
            s.setString(1, id.getProducerId().toString());
            s.setLong(2, id.getProducerSequenceId());
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

        } finally {
            cleanupExclusiveLock.readLock().unlock();
            close(rs);
            close(s);
        }
    }

}
