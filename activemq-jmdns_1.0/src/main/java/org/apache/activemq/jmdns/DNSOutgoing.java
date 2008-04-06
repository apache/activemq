//Copyright 2003-2005 Arthur van Hoff, Rick Blair
//Licensed under Apache License version 2.0
//Original license LGPL


package org.apache.activemq.jmdns;

import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An outgoing DNS message.
 *
 * @version %I%, %G%
 * @author	Arthur van Hoff, Rick Blair, Werner Randelshofer
 */
final class DNSOutgoing
{
    private static Logger logger = Logger.getLogger(DNSOutgoing.class.toString());
    int id;
    int flags;
    private boolean multicast;
    private int numQuestions;
    private int numAnswers;
    private int numAuthorities;
    private int numAdditionals;
    private Hashtable names;

    byte data[];
    int off;
    int len;

    /**
     * Create an outgoing multicast query or response.
     */
    DNSOutgoing(int flags)
    {
        this(flags, true);
    }

    /**
     * Create an outgoing query or response.
     */
    DNSOutgoing(int flags, boolean multicast)
    {
        this.flags = flags;
        this.multicast = multicast;
        names = new Hashtable();
        data = new byte[DNSConstants.MAX_MSG_TYPICAL];
        off = 12;
    }

    /**
     * Add a question to the message.
     */
    void addQuestion(DNSQuestion rec) throws IOException
    {
        if (numAnswers > 0 || numAuthorities > 0 || numAdditionals > 0)
        {
            throw new IllegalStateException("Questions must be added before answers");
        }
        numQuestions++;
        writeQuestion(rec);
    }

    /**
     * Add an answer if it is not suppressed.
     */
    void addAnswer(DNSIncoming in, DNSRecord rec) throws IOException
    {
        if (numAuthorities > 0 || numAdditionals > 0)
        {
            throw new IllegalStateException("Answers must be added before authorities and additionals");
        }
        if (!rec.suppressedBy(in))
        {
            addAnswer(rec, 0);
        }
    }

    /**
     * Add an additional answer to the record. Omit if there is no room.
     */
    void addAdditionalAnswer(DNSIncoming in, DNSRecord rec) throws IOException
    {
        if ((off < DNSConstants.MAX_MSG_TYPICAL - 200) && !rec.suppressedBy(in))
        {
            writeRecord(rec, 0);
            numAdditionals++;
        }
    }

    /**
     * Add an answer to the message.
     */
    void addAnswer(DNSRecord rec, long now) throws IOException
    {
        if (numAuthorities > 0 || numAdditionals > 0)
        {
            throw new IllegalStateException("Questions must be added before answers");
        }
        if (rec != null)
        {
            if ((now == 0) || !rec.isExpired(now))
            {
                writeRecord(rec, now);
                numAnswers++;
            }
        }
    }

    private LinkedList authorativeAnswers = new LinkedList();

    /**
     * Add an authorative answer to the message.
     */
    void addAuthorativeAnswer(DNSRecord rec) throws IOException
    {
        if (numAdditionals > 0)
        {
            throw new IllegalStateException("Authorative answers must be added before additional answers");
        }
        authorativeAnswers.add(rec);
        writeRecord(rec, 0);
        numAuthorities++;

        // VERIFY:

    }

    void writeByte(int value) throws IOException
    {
        if (off >= data.length)
        {
            throw new IOException("buffer full");
        }
        data[off++] = (byte) value;
    }

    void writeBytes(String str, int off, int len) throws IOException
    {
        for (int i = 0; i < len; i++)
        {
            writeByte(str.charAt(off + i));
        }
    }

    void writeBytes(byte data[]) throws IOException
    {
        if (data != null)
        {
            writeBytes(data, 0, data.length);
        }
    }

    void writeBytes(byte data[], int off, int len) throws IOException
    {
        for (int i = 0; i < len; i++)
        {
            writeByte(data[off + i]);
        }
    }

    void writeShort(int value) throws IOException
    {
        writeByte(value >> 8);
        writeByte(value);
    }

    void writeInt(int value) throws IOException
    {
        writeShort(value >> 16);
        writeShort(value);
    }

    void writeUTF(String str, int off, int len) throws IOException
    {
        // compute utf length
        int utflen = 0;
        for (int i = 0; i < len; i++)
        {
            int ch = str.charAt(off + i);
            if ((ch >= 0x0001) && (ch <= 0x007F))
            {
                utflen += 1;
            }
            else
            {
                if (ch > 0x07FF)
                {
                    utflen += 3;
                }
                else
                {
                    utflen += 2;
                }
            }
        }
        // write utf length
        writeByte(utflen);
        // write utf data
        for (int i = 0; i < len; i++)
        {
            int ch = str.charAt(off + i);
            if ((ch >= 0x0001) && (ch <= 0x007F))
            {
                writeByte(ch);
            }
            else
            {
                if (ch > 0x07FF)
                {
                    writeByte(0xE0 | ((ch >> 12) & 0x0F));
                    writeByte(0x80 | ((ch >> 6) & 0x3F));
                    writeByte(0x80 | ((ch >> 0) & 0x3F));
                }
                else
                {
                    writeByte(0xC0 | ((ch >> 6) & 0x1F));
                    writeByte(0x80 | ((ch >> 0) & 0x3F));
                }
            }
        }
    }

    void writeName(String name) throws IOException
    {
        while (true)
        {
            int n = name.indexOf('.');
            if (n < 0)
            {
                n = name.length();
            }
            if (n <= 0)
            {
                writeByte(0);
                return;
            }
            Integer offset = (Integer) names.get(name);
            if (offset != null)
            {
                int val = offset.intValue();

                if (val > off)
                {
                    logger.log(Level.WARNING, "DNSOutgoing writeName failed val=" + val + " name=" + name);
                }

                writeByte((val >> 8) | 0xC0);
                writeByte(val);
                return;
            }
            names.put(name, new Integer(off));
            writeUTF(name, 0, n);
            name = name.substring(n);
            if (name.startsWith("."))
            {
                name = name.substring(1);
            }
        }
    }

    void writeQuestion(DNSQuestion question) throws IOException
    {
        writeName(question.name);
        writeShort(question.type);
        writeShort(question.clazz);
    }

    void writeRecord(DNSRecord rec, long now) throws IOException
    {
        int save = off;
        try
        {
            writeName(rec.name);
            writeShort(rec.type);
            writeShort(rec.clazz | ((rec.unique && multicast) ? DNSConstants.CLASS_UNIQUE : 0));
            writeInt((now == 0) ? rec.ttl : rec.getRemainingTTL(now));
            writeShort(0);
            int start = off;
            rec.write(this);
            int len = off - start;
            data[start - 2] = (byte) (len >> 8);
            data[start - 1] = (byte) (len & 0xFF);
        }
        catch (IOException e)
        {
            off = save;
            throw e;
        }
    }

    /**
     * Finish the message before sending it off.
     */
    void finish() throws IOException
    {
        int save = off;
        off = 0;

        writeShort(multicast ? 0 : id);
        writeShort(flags);
        writeShort(numQuestions);
        writeShort(numAnswers);
        writeShort(numAuthorities);
        writeShort(numAdditionals);
        off = save;
    }

    boolean isQuery()
    {
        return (flags & DNSConstants.FLAGS_QR_MASK) == DNSConstants.FLAGS_QR_QUERY;
    }

    public boolean isEmpty()
    {
        return numQuestions == 0 && numAuthorities == 0
            && numAdditionals == 0 && numAnswers == 0;
    }


    public String toString()
    {
        StringBuffer buf = new StringBuffer();
        buf.append(isQuery() ? "dns[query," : "dns[response,");
        //buf.append(packet.getAddress().getHostAddress());
        buf.append(':');
        //buf.append(packet.getPort());
        //buf.append(",len=");
        //buf.append(packet.getLength());
        buf.append(",id=0x");
        buf.append(Integer.toHexString(id));
        if (flags != 0)
        {
            buf.append(",flags=0x");
            buf.append(Integer.toHexString(flags));
            if ((flags & DNSConstants.FLAGS_QR_RESPONSE) != 0)
            {
                buf.append(":r");
            }
            if ((flags & DNSConstants.FLAGS_AA) != 0)
            {
                buf.append(":aa");
            }
            if ((flags & DNSConstants.FLAGS_TC) != 0)
            {
                buf.append(":tc");
            }
        }
        if (numQuestions > 0)
        {
            buf.append(",questions=");
            buf.append(numQuestions);
        }
        if (numAnswers > 0)
        {
            buf.append(",answers=");
            buf.append(numAnswers);
        }
        if (numAuthorities > 0)
        {
            buf.append(",authorities=");
            buf.append(numAuthorities);
        }
        if (numAdditionals > 0)
        {
            buf.append(",additionals=");
            buf.append(numAdditionals);
        }
        buf.append(",\nnames=" + names);
        buf.append(",\nauthorativeAnswers=" + authorativeAnswers);

        buf.append("]");
        return buf.toString();
    }

}
