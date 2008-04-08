//Copyright 2003-2005 Arthur van Hoff, Rick Blair
//Licensed under Apache License version 2.0
//Original license LGPL


package org.apache.activemq.jmdns;

import java.util.logging.Logger;

/**
 * A DNS question.
 *
 * @version %I%, %G%
 * @author	Arthur van Hoff
 */
final class DNSQuestion extends DNSEntry
{
    private static Logger logger = Logger.getLogger(DNSQuestion.class.toString());

    /**
     * Create a question.
     */
    DNSQuestion(String name, int type, int clazz)
    {
        super(name, type, clazz);
    }

    /**
     * Check if this question is answered by a given DNS record.
     */
    boolean answeredBy(DNSRecord rec)
    {
        return (clazz == rec.clazz) && ((type == rec.type) || (type == DNSConstants.TYPE_ANY)) &&
            name.equals(rec.name);
    }

    /**
     * For debugging only.
     */
    public String toString()
    {
        return toString("question", null);
    }
}
