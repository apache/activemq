<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
                xmlns:c="http://jmscts.sourceforge.net/coverage"
                xmlns:t="http://jmscts.sourceforge.net/test"
                xmlns:redirect="http://xml.apache.org/xalan/redirect"
                extension-element-prefixes="redirect">


  <xsl:output method="xml" indent="yes"/>

  <xsl:variable name="meta" select="document('metadata.xml')"/>

  <xsl:template name="output-test">
    <redirect:write select="concat('xdocs/', test, '.xml')">
      <document>
        <body>
          <section name="{test}">
            <xsl:call-template name="output-description"/>
            <xsl:call-template name="output-test-runs"/>
            <xsl:if test="t:testRun/t:failure">
              <xsl:call-template name="output-failures"/>
            </xsl:if>
          </section>
        </body>
      </document>
    </redirect:write>
  </xsl:template>

  <xsl:template name="output-description">
    <xsl:apply-templates select="$meta/meta-data">
      <xsl:with-param name="name" select="test"/>
    </xsl:apply-templates>
  </xsl:template>

  <xsl:key name="method-desc" match="/meta-data/class-meta/method-meta"
           use="concat(../name, '.', name)"/>

  <xsl:template match="meta-data">
    <xsl:param name="name"/>
    <xsl:apply-templates select="key('method-desc', $name)"/>
  </xsl:template>

  <xsl:template match="method-meta">
    <p>
      <xsl:copy-of select="description/node()"/>
    </p>
    <p>
      <small>Requirements:</small>
      <ul>
      <xsl:for-each select="attribute[@name='jmscts.requirement']">
        <li>
          <a href="{@value}.html">
            <small><xsl:value-of select="@value"/></small>
          </a>
        </li>
      </xsl:for-each>
      </ul>
    </p>
  </xsl:template>

  <xsl:template name="output-test-runs">
    <subsection name="Runs">
      <p>
        <table>
          <tr>
            <th>Run</th>
            <th>ConnectionFactory</th>
            <th>Destination</th>
            <th>Delivery</th>
            <th>Session</th>
            <th>Consumer</th>
            <th>Message</th>
            <th>Pass</th>
          </tr>
          <xsl:apply-templates select="t:testRun"/>
        </table>
      </p>
    </subsection>
  </xsl:template>
         
  <xsl:template name="output-failures">
    <subsection name="Failures">
      <p>
        <table>
          <tr>
            <th>Run</th>
            <th>Description</th>
          </tr>
          <xsl:for-each select="t:testRun">
            <xsl:if test="t:failure">
              <tr>
                <td>
                  <a name="failure{position()}">
                    <a href="#run{position()}">
                      <small><xsl:value-of select="position()"/></small>
                    </a>
                  </a>
                </td>
                <td>
                  <small><xsl:value-of select="t:failure/description"/></small>
                </td>
              </tr>
              <tr>
                <td/>
                <td>
                  <pre><small><xsl:value-of select="t:failure/cause"/></small></pre>
                </td>
              </tr>
            </xsl:if>
          </xsl:for-each>
        </table>
      </p>
    </subsection>
  </xsl:template>

  <xsl:template match="t:testRun">
    <xsl:variable name="run" select="position()"/>

    <tr>
      <td>
        <a name="run{$run}"><small><xsl:value-of select="$run"/></small></a>
      </td> 
      <td>
        <small><xsl:value-of select="t:context/t:factory/@type"/></small>
      </td>
      <td>
        <small>
          <xsl:value-of select="t:context/t:behaviour/@destination"/>
        </small>
      </td>
      <td>
        <small>
          <xsl:value-of select="t:context/t:behaviour/@deliveryMode"/>
        </small>
      </td>
      <td>
        <small>
          <xsl:call-template name="get-session">
            <xsl:with-param name="session" select="t:context/session"/>
          </xsl:call-template>
        </small>
      </td>
      <td>
        <small>
          <xsl:call-template name="get-consumer">
            <xsl:with-param name="consumer" 
                            select="t:context/t:behaviour/@receiver"/>
          </xsl:call-template>
        </small>
      </td>
      <td><small><xsl:value-of select="t:context/message"/></small></td>
      <td>
        <small>
          <xsl:choose>
            <xsl:when test="t:failure">
              <a href="#failure{$run}">No</a>
            </xsl:when>
            <xsl:otherwise>Yes</xsl:otherwise>
          </xsl:choose>
        </small>
      </td>
    </tr>
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

