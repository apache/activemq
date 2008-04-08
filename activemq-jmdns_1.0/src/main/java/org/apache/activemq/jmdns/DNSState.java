//Copyright 2003-2005 Arthur van Hoff, Rick Blair
//Licensed under Apache License version 2.0
//Original license LGPL

package org.apache.activemq.jmdns;

import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * DNSState defines the possible states for services registered with JmDNS.
 *
 * @author Werner Randelshofer, Rick Blair
 * @version 1.0  May 23, 2004  Created.
 */
public class DNSState implements Comparable
{
    private static Logger logger = Logger.getLogger(DNSState.class.toString());

    private final String name;

    /**
     * Ordinal of next state to be created.
     */
    private static int nextOrdinal = 0;
    /**
     * Assign an ordinal to this state.
     */
    private final int ordinal = nextOrdinal++;
    /**
     * Logical sequence of states.
     * The sequence is consistent with the ordinal of a state.
     * This is used for advancing through states.
     */
    private final static ArrayList sequence = new ArrayList();

    private DNSState(String name)
    {
        this.name = name;
        sequence.add(this);
    }

    public final String toString()
    {
        return name;
    }

    public static final DNSState PROBING_1 = new DNSState("probing 1");
    public static final DNSState PROBING_2 = new DNSState("probing 2");
    public static final DNSState PROBING_3 = new DNSState("probing 3");
    public static final DNSState ANNOUNCING_1 = new DNSState("announcing 1");
    public static final DNSState ANNOUNCING_2 = new DNSState("announcing 2");
    public static final DNSState ANNOUNCED = new DNSState("announced");
    public static final DNSState CANCELED = new DNSState("canceled");

    /**
     * Returns the next advanced state.
     * In general, this advances one step in the following sequence: PROBING_1,
     * PROBING_2, PROBING_3, ANNOUNCING_1, ANNOUNCING_2, ANNOUNCED.
     * Does not advance for ANNOUNCED and CANCELED state.
     */
    public final DNSState advance()
    {
        return (isProbing() || isAnnouncing()) ? (DNSState) sequence.get(ordinal + 1) : this;
    }

    /**
     * Returns to the next reverted state.
     * All states except CANCELED revert to PROBING_1.
     * Status CANCELED does not revert.
     */
    public final DNSState revert()
    {
        return (this == CANCELED) ? this : PROBING_1;
    }

    /**
     * Returns true, if this is a probing state.
     */
    public boolean isProbing()
    {
        return compareTo(PROBING_1) >= 0 && compareTo(PROBING_3) <= 0;
    }

    /**
     * Returns true, if this is an announcing state.
     */
    public boolean isAnnouncing()
    {
        return compareTo(ANNOUNCING_1) >= 0 && compareTo(ANNOUNCING_2) <= 0;
    }

    /**
     * Returns true, if this is an announced state.
     */
    public boolean isAnnounced()
    {
        return compareTo(ANNOUNCED) == 0;
    }

    /**
     * Compares two states.
     * The states compare as follows:
     * PROBING_1 &lt; PROBING_2 &lt; PROBING_3 &lt; ANNOUNCING_1 &lt;
     * ANNOUNCING_2 &lt; RESPONDING &lt; ANNOUNCED &lt; CANCELED.
     */
    public int compareTo(Object o)
    {
        return ordinal - ((DNSState) o).ordinal;
    }
}