<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
                xmlns:t="http://jmscts.sourceforge.net/test"
                xmlns:redirect="http://xml.apache.org/xalan/redirect"
                extension-element-prefixes="redirect">

  <xsl:output method="xml" indent="yes"/>

  <xsl:variable name="meta" select="document('metadata.xml')"/>
  <xsl:variable name="req" select="document('requirements.xml')"/>


  <xsl:template name="output-requirement">   
    <xsl:variable name="root" select="parent::*"/>
    <xsl:variable name="reqId" select="requirementId"/>

    <redirect:write select="concat('xdocs/', $reqId, '.xml')">
      <document>
        <properties>
          <title>
            <xsl:value-of select="concat('Requirement: ',$reqId)"/>
          </title>
        </properties>
        <body>
          <section name="Requirement: {$reqId}">
            <xsl:for-each 
               select="$req/document/requirement[@requirementId=$reqId]">
               <xsl:call-template name="output-requirement-detail">
                 <xsl:with-param name="root" select="$root"/>
               </xsl:call-template>
            </xsl:for-each>
          </section>
        </body>
      </document>
    </redirect:write>
  </xsl:template>

  <xsl:template name="output-requirement-detail">
    <xsl:param name="root"/>

    <xsl:variable name="reqId" select="@requirementId"/>
    
    <p>
      <small><xsl:copy-of select="description/node()"/></small>
    </p>
    <p>
      <small>See:</small>
      <ul>
        <xsl:apply-templates select="referenceId"/>
        <xsl:apply-templates select="reference"/>
      </ul>
    </p>
    <subsection name="Test cases">
      <p>
        <table>
          <tr><th>Test Case</th><th>Tests</th><th>Failures</th></tr>
          <xsl:for-each 
               select="$meta//method-meta/attribute[@name='jmscts.requirement' 
                       and @value=$reqId]">
            <xsl:sort select="concat(../../name, '.', ../name)"/>
            <xsl:variable name="class" select="../../name"/>
            <xsl:variable name="method" select="../name"/>
            <xsl:variable name="test" select="concat($class,'.',$method)"/>

            <xsl:variable name="testRuns" 
                          select="$root/t:testRuns[test = $test]"/>
            <tr>
              <td>
                <a href="{$test}.html">
                  <small><xsl:value-of select="$test"/></small>
                </a>
              </td>
              <td>
                <small>
                  <xsl:value-of select="count($testRuns/t:testRun)"/>
                </small>
              </td>
              <td>
                <small>
                  <xsl:value-of select="count($testRuns/t:testRun/t:failure)"/>
                </small>
              </td>
            </tr>
          </xsl:for-each>
        </table>
      </p>  
    </subsection>
  </xsl:template>

  <xsl:template match="referenceId">
    <xsl:variable name="id" select="." />
    <xsl:apply-templates 
         select="ancestor::document/reference[@referenceId=$id]"/>
  </xsl:template>

  <xsl:template match="reference">
    <li>
      <small>
        <xsl:choose>
          <xsl:when test="section">
            Section&#160;<xsl:value-of select="section/@name"/>,
            <xsl:value-of select="section/@title"/>
          </xsl:when>
          <xsl:when test="table">
            Table <xsl:value-of select="table"/>
          </xsl:when> 
          <xsl:when test="url">
            <a href="{url}"><xsl:value-of select="url"/></a>
          </xsl:when>     
        </xsl:choose>
      </small>
    </li>
  </xsl:template>

</xsl:stylesheet>
