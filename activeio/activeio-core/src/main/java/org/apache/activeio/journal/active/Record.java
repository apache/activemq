/**
 *
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
package org.apache.activeio.journal.active;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;

import org.apache.activeio.adapter.PacketOutputStream;
import org.apache.activeio.adapter.PacketToInputStream;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.Packet;


/**
 * Serializes/Deserializes data records. 
 * 
 * @version $Revision: 1.1 $
 */
final public class Record {
    
    static final public int RECORD_HEADER_SIZE=8+Location.SERIALIZED_SIZE;
    static final public int RECORD_FOOTER_SIZE=12+Location.SERIALIZED_SIZE;
	static final public int RECORD_BASE_SIZE=RECORD_HEADER_SIZE+RECORD_FOOTER_SIZE;
	
    static final public byte[] START_OF_RECORD 	= new byte[] { 'S', 'o', 'R' }; 
    static final public byte[] END_OF_RECORD 	= new byte[] { 'E', 'o', 'R', '.' }; 
        
	static final public int SELECTED_CHECKSUM_ALGORITHIM;
	static final public int NO_CHECKSUM_ALGORITHIM=0;
	static final public int HASH_CHECKSUM_ALGORITHIM=1;
	static final public int CRC32_CHECKSUM_ALGORITHIM=2;
	
	static {
		String type = System.getProperty("org.apache.activeio.journal.active.SELECTED_CHECKSUM_ALGORITHIM", "none");
		if( "none".equals(type) ) {
			SELECTED_CHECKSUM_ALGORITHIM = NO_CHECKSUM_ALGORITHIM;			
		} else if( "crc32".equals(type) ) {
			SELECTED_CHECKSUM_ALGORITHIM = CRC32_CHECKSUM_ALGORITHIM;			
		} else if( "hash".equals(type) ) {
			SELECTED_CHECKSUM_ALGORITHIM = HASH_CHECKSUM_ALGORITHIM;			
		} else {
			System.err.println("System property 'org.apache.activeio.journal.active.SELECTED_CHECKSUM_ALGORITHIM' not set properly.  Valid values are: 'none', 'hash', or 'crc32'");
			SELECTED_CHECKSUM_ALGORITHIM = NO_CHECKSUM_ALGORITHIM;			
		}
	}
	
	static public boolean isChecksumingEnabled() {
		return SELECTED_CHECKSUM_ALGORITHIM!=NO_CHECKSUM_ALGORITHIM;
	}
		    
    private final ByteArrayPacket headerFooterPacket = new ByteArrayPacket(new byte[RECORD_BASE_SIZE]);
    private final DataOutputStream headerFooterData = new DataOutputStream(new PacketOutputStream(headerFooterPacket));

    private int payloadLength;
    private Location location;
    private byte recordType;        
    private long checksum;	
    private Location mark;
    private Packet payload;
 		
    public Record() {        
    }

    public Record(byte recordType, Packet payload, Location mark) throws IOException {
        this(null, recordType, payload, mark);
    }
    
    public Record(Location location, byte recordType, Packet payload, Location mark) throws IOException {
        this.location = location;
        this.recordType = recordType;
        this.mark = mark;
        this.payload = payload.slice();
        this.payloadLength = payload.remaining();
        if( isChecksumingEnabled() ) {
            checksum(new DataInputStream(new PacketToInputStream(this.payload)));
        }

        writeHeader(headerFooterData);
        writeFooter(headerFooterData);
    }    
    
    public void setLocation(Location location) throws IOException {
        this.location = location;
        headerFooterPacket.clear();
        headerFooterPacket.position(8);
        location.writeToDataOutput(headerFooterData);
        headerFooterPacket.position(RECORD_HEADER_SIZE+8);
        location.writeToDataOutput(headerFooterData);
        payload.clear();
        headerFooterPacket.position(0);
        headerFooterPacket.limit(RECORD_HEADER_SIZE);
    }
    
	private void writeHeader( DataOutput out ) throws IOException {
	    out.write(START_OF_RECORD);
	    out.writeByte(recordType);
	    out.writeInt(payloadLength);
        if( location!=null )
            location.writeToDataOutput(out);
        else
            out.writeLong(0);
	}	
	
	public void readHeader( DataInput in ) throws IOException {
        readAndCheckConstant(in, START_OF_RECORD, "Invalid record header: start of record constant missing.");
        recordType = in.readByte();
        payloadLength = in.readInt();
        if( payloadLength < 0 )
            throw new IOException("Invalid record header: record length cannot be less than zero.");
        location = Location.readFromDataInput(in);
	}
	
	private void writeFooter( DataOutput out ) throws IOException {
	    out.writeLong(checksum);
        if( location!=null )
            location.writeToDataOutput(out);
        else
            out.writeLong(0);
	    out.write(END_OF_RECORD);
	}
	
	public void readFooter( DataInput in ) throws IOException {
	    long l = in.readLong();	    
        if( isChecksumingEnabled() ) {
            if( l!=checksum )            
                throw new IOException("Invalid record footer: checksum does not match.");
        } else {
            checksum = l;            
        }
        
        Location loc = Location.readFromDataInput(in);
        if( !loc.equals(location) )
            throw new IOException("Invalid record footer: location id does not match.");
        
        readAndCheckConstant(in, END_OF_RECORD, "Invalid record header: end of record constant missing.");
	}
	
    /**
     * @param randomAccessFile
     * @throws IOException
     */
	public void checksum(DataInput in) throws IOException {
		if( SELECTED_CHECKSUM_ALGORITHIM==HASH_CHECKSUM_ALGORITHIM ) {

		    byte  buffer[] = new byte[1024];
			byte rc[] = new byte[8];
			for (int i = 0; i < payloadLength;) {
			    int l = Math.min(buffer.length, payloadLength-i);
				in.readFully(buffer,0,l);
				for (int j = 0; j < l; j++) {
					rc[j%8] ^= buffer[j];			
				}
				i+=l;
			}			
			checksum = (rc[0])|(rc[1]<<1)|(rc[2]<<2)|(rc[3]<<3)|(rc[4]<<4)|(rc[5]<<5)|(rc[6]<<6)|(rc[7]<<7) ;
			
		} else if( SELECTED_CHECKSUM_ALGORITHIM==CRC32_CHECKSUM_ALGORITHIM ) {
			byte  buffer[] = new byte[1024];
			CRC32 crc32 = new CRC32();
			for (int i = 0; i < payloadLength;) {
			    int l = Math.min(buffer.length, payloadLength-i);
				in.readFully(buffer,0,l);
				crc32.update(buffer,0,l);
				i+=l;
			}			
			checksum = crc32.getValue();
		} else {
		    checksum = 0L;
		}
    }

	
    /**
     */
    private void readAndCheckConstant(DataInput in, byte[] byteConstant, String errorMessage ) throws IOException {
        for (int i = 0; i < byteConstant.length; i++) {
            byte checkByte = byteConstant[i];
            if( in.readByte()!= checkByte ) {
                throw new IOException(errorMessage);
            }
        }
    }    
    
    public boolean readFromPacket(Packet packet) throws IOException {
        Packet dup = packet.duplicate();

        if( dup.remaining() < RECORD_HEADER_SIZE )
            return false;
        DataInputStream is = new DataInputStream(new PacketToInputStream(dup));
        readHeader( is );
        if( dup.remaining() < payloadLength+RECORD_FOOTER_SIZE ) {
            return false;
        }
        
        // Set limit to create a slice of the payload.
        dup.limit(dup.position()+payloadLength);
        this.payload = dup.slice();        
	    if( isChecksumingEnabled() ) {
	        checksum(new DataInputStream(new PacketToInputStream(payload)));
	    }
	    
	    // restore the limit and seek to the footer.
        dup.limit(packet.limit());
        dup.position(dup.position()+payloadLength);
        readFooter(is);
        
        // If every thing went well.. advance the position of the orignal packet.
        packet.position(dup.position());
        dup.dispose();
        return true;        
    }
    
    /**
     * @return Returns the checksum.
     */
    public long getChecksum() {
        return checksum;
    }

    /**
     * @return Returns the length.
     */
    public int getPayloadLength() {
        return payloadLength;
    }

    /**
     * @return Returns the length of the record .
     */
    public int getRecordLength() {
        return payloadLength+Record.RECORD_BASE_SIZE;
    }

    /**
     * @return Returns the location.
     */
    public Location getLocation() {
        return location;
    }
    
    /**
     * @return Returns the mark.
     */
    public Location getMark() {
        return mark;
    }

    /**
     * @return Returns the payload.
     */
    public Packet getPayload() {
        return payload;
    }

    /**
     * @return Returns the recordType.
     */
    public byte getRecordType() {
        return recordType;
    }

	public boolean hasRemaining() {
		return headerFooterPacket.position()!=RECORD_BASE_SIZE;
	}

	public void read(Packet packet) {
		
		// push the header
		headerFooterPacket.read(packet);
		// push the payload.
		payload.read(packet);
		
		// Can we switch to the footer now?
		if( !payload.hasRemaining() && headerFooterPacket.position()==RECORD_HEADER_SIZE ) {
			headerFooterPacket.position(RECORD_HEADER_SIZE);
             headerFooterPacket.limit(RECORD_BASE_SIZE);
			headerFooterPacket.read(packet);			
		}
		
	}

    public void dispose() {
        if( payload!=null ) {
            payload.dispose();
            payload=null;
        }
    }

}
