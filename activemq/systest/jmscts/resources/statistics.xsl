<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
                xmlns:s="http://jmscts.sourceforge.net/statistics"
                xmlns:t="http://jmscts.sourceforge.net/test">

  <xsl:output method="xml" indent="yes"/>

  <xsl:variable name="meta" select="document('metadata.xml')"/>

  <xsl:template match="/">
    <document>
      <properties>
        <title>Statistics</title>
      </properties>
      <xsl:apply-templates select="s:statistics"/>
    </document>
  </xsl:template>

  <xsl:template match="s:statistics">
    <body>
      <xsl:apply-templates select="t:testRuns"/>
    </body>
  </xsl:template>

  <xsl:template match="t:testRuns">    

    <section name="Statistics for test: {test}">
      <xsl:call-template name="print-description"/>
      <xsl:call-template name="print-runs">
        <xsl:with-param name="title" select="'Queue'"/>
        <xsl:with-param name="factory" select="'QueueConnectionFactory'"/>
        <xsl:with-param name="destType" select="'administered'"/>
      </xsl:call-template>

      <xsl:call-template name="print-runs">
        <xsl:with-param name="title" select="'TemporaryQueue'"/>
        <xsl:with-param name="factory" select="'QueueConnectionFactory'"/>
        <xsl:with-param name="destType" select="'temporary'"/>
      </xsl:call-template>

      <xsl:call-template name="print-runs">
        <xsl:with-param name="title" select="'Topic'"/>
        <xsl:with-param name="factory" select="'TopicConnectionFactory'"/>
        <xsl:with-param name="destType" select="'administered'"/>
      </xsl:call-template>

      <xsl:call-template name="print-runs">
        <xsl:with-param name="title" select="'TemporaryTopic'"/>
        <xsl:with-param name="factory" select="'TopicConnectionFactory'"/>
        <xsl:with-param name="destType" select="'temporary'"/>
      </xsl:call-template>
    </section>
  </xsl:template>

  <xsl:template name="print-description">
    <p>      
      <xsl:apply-templates select="$meta/meta-data">
        <xsl:with-param name="name" select="test"/>
      </xsl:apply-templates>
    </p>
  </xsl:template>

  <xsl:key name="method-desc" match="/meta-data/class-meta/method-meta"
           use="concat(../name, '.', name)"/>

  <xsl:template match="meta-data">
    <xsl:param name="name"/>
    <xsl:apply-templates select="key('method-desc', $name)"/>
  </xsl:template>

  <xsl:template match="method-meta">
    <xsl:copy-of select="description/node()"/>
  </xsl:template>

  <xsl:template name="print-runs">
    <xsl:param name="title"/>
    <xsl:param name="factory"/>
    <xsl:param name="destType"/>

    <xsl:variable name="has-consumer">
      <xsl:choose>
        <xsl:when test="count(t:testRun/t:statistic[type='receive']) > 0">true</xsl:when>
        <xsl:otherwise>false</xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <subsection name="{$title}">
      <p>
        <table>
          <tr>
            <th>Run</th>
            <th>Delivery</th>
            <th>Session</th>
            <xsl:if test="$has-consumer='true'">
              <th>Consumer</th>
            </xsl:if>
            <th>Message</th>
            <th>Type</th>
            <th>Count</th>
            <th>Time</th>
            <th>Msgs/sec</th>
          </tr>          
          <xsl:apply-templates 
               select="t:testRun[t:context/t:factory/@type=$factory and
                                t:context/t:behaviour/@destination=$destType]">
            <xsl:sort select="t:context/t:behaviour/@deliveryMode"/>
            <xsl:with-param name="has-consumer" select="$has-consumer"/>
          </xsl:apply-templates>
        </table>
      </p>
    </subsection>
  </xsl:template>

  <xsl:template match="t:testRun">
    <xsl:param name="has-consumer"/>

    <xsl:apply-templates select="t:statistic">
      <xsl:with-param name="run" select="position()"/>
      <xsl:with-param name="has-consumer" select="$has-consumer"/>
    </xsl:apply-templates>
  </xsl:template>

  <xsl:template match="t:statistic">
    <xsl:param name="run"/>
    <xsl:param name="has-consumer"/>
    <tr>      
     <xsl:choose>
        <xsl:when test="position() = 1">
          <xsl:call-template name="statistic-header">
            <xsl:with-param name="run" select="$run"/>
            <xsl:with-param name="has-consumer" select="$has-consumer"/>
          </xsl:call-template>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="statistic">
            <xsl:with-param name="has-consumer" select="$has-consumer"/>
          </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
    </tr>
  </xsl:template>

  <xsl:template name="statistic-header">           
    <xsl:param name="run"/>
    <xsl:param name="has-consumer"/>
    <td> 
      <small><xsl:value-of select="$run"/></small>
    </td> 
    <td>
      <small><xsl:value-of select="..//t:behaviour/@deliveryMode"/></small>
    </td>
    <td>
      <small>
        <xsl:call-template name="get-session">
          <xsl:with-param name="session" select="..//session"/>
        </xsl:call-template>
      </small>
    </td>
    <xsl:if test="$has-consumer='true'">
      <td>
        <small>
          <xsl:call-template name="get-consumer">
            <xsl:with-param name="consumer" 
                            select="..//t:behaviour/@receiver"/>
          </xsl:call-template>
        </small>
      </td>
    </xsl:if>
    <td>
      <small><xsl:value-of select="..//message"/></small>
    </td>
    <td><small><xsl:value-of select="type"/></small></td>
    <td><small><xsl:value-of select="count"/></small></td>
    <td><small><xsl:value-of select="time"/></small></td>
    <td>
      <small>
        <xsl:value-of select="format-number(rate, '0.00')"/>
      </small>
    </td>
  </xsl:template>

  <xsl:template name="statistic">
    <xsl:param name="has-consumer"/>
    <td/>     <!-- empty run column -->
    <td/>     <!-- empty delivery column -->
    <td/>     <!-- empty session column -->
    <xsl:if test="$has-consumer='true'">
      <td/>   <!-- empty consumer column -->
    </xsl:if>
    <td/>     <!-- empty message column -->
    <td><small><xsl:value-of select="type"/></small></td>
    <td><small><xsl:value-of select="count"/></small></td>
    <td><small><xsl:value-of select="time"/></small></td>
    <td>
      <small>
        <xsl:value-of select="format-number(rate, '0.00')"/>
      </small>
    </td>
  </xsl:template>

  <xsl:template name="get-session">
    <xsl:param name="session"/>

    <xsl:choose>
      <xsl:when test="$session='AUTO_ACKNOWLEDGE'">AUTO</xsl:when>
      <xsl:when test="$session='CLIENT_ACKNOWLEDGE'">CLIENT</xsl:when>
      <xsl:when test="$session='DUPS_OK_ACKNOWLEDGE'">DUPS_OK</xsl:when>
      <xsl:otherwise><xsl:value-of select="$session"/></xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="get-consumer">
    <xsl:param name="consumer"/>

    <xsl:choose>
      <xsl:when test="$consumer='durable_synchronous'">
        durable synchronous
      </xsl:when>
      <xsl:when test="$consumer='durable_asynchronous'">
        durable asynchronous
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="$consumer"/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>
