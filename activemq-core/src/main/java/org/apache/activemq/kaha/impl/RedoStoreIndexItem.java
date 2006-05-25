package org.apache.activemq.kaha.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.activemq.kaha.Marshaller;

public class RedoStoreIndexItem implements Externalizable {

    public static final Marshaller MARSHALLER = new Marshaller() {
        public Object readPayload(DataInput in) throws IOException {
            RedoStoreIndexItem item = new RedoStoreIndexItem();
            item.readExternal(in);
            return item;
        }

        public void writePayload(Object object, DataOutput out) throws IOException {
            RedoStoreIndexItem item = (RedoStoreIndexItem) object;
            item.writeExternal(out);
        }
    }; 
    
    private static final long serialVersionUID = -4865508871719676655L;
    private String indexName;
    private IndexItem indexItem;
    private long offset;

    public RedoStoreIndexItem() {
    }
    public RedoStoreIndexItem(String indexName, long offset, IndexItem item) {
        this.indexName = indexName;
        this.offset=offset;
        this.indexItem = item;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternal((DataInput)in);
    }
    public void readExternal(DataInput in) throws IOException {
        // indexName = in.readUTF();
        offset = in.readLong();
        indexItem = new IndexItem();
        indexItem.read(in);
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        writeExternal((DataOutput)out);
    }
    public void writeExternal(DataOutput out) throws IOException {
        // out.writeUTF(indexName);
        out.writeLong(offset);
        indexItem.write(out);
    }
    
    public String getIndexName() {
        return indexName;
    }
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    
    public IndexItem getIndexItem() {
        return indexItem;
    }
    public void setIndexItem(IndexItem item) {
        this.indexItem = item;
    }
    public long getOffset() {
        return offset;
    }
    public void setOffset(long offset) {
        this.offset = offset;
    }

}
