<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
  <xsl:param name="style-path"/>

  <xsl:output method="html" indent="yes"/>

  <xsl:template match="document">
    <xsl:variable name="tigris-css" 
                  select="concat($style-path, '/tigris.css')"/>
    <xsl:variable name="maven-css" select="concat($style-path, '/maven.css')"/>
    <html>
   
      <style type="text/css">
        @import url("<xsl:value-of select="$tigris-css"/>");
        @import url("<xsl:value-of select="$maven-css"/>");
      </style>

      <head>
        <title><xsl:value-of select="properties/title"/></title>
      </head>

      <xsl:apply-templates select="body"/>

    </html>
  </xsl:template>
  
  <xsl:template match="body">
    <body>
      <table border="0" cellspacing="0" cellpadding="8" width="100%">
        <tr valign="top">
          <td rowspan="2">
            <div id="bodycol">
              <div class="app">
                <xsl:apply-templates select="section"/>
              </div>
            </div>
          </td>
        </tr>
      </table>
    </body>
  </xsl:template>

  <!-- process a documentation section -->
  <xsl:template match="section">
    <div class="h3">
      <xsl:if test="@name">
        <h3>
          <a name="{@name}"><xsl:value-of select="@name"/></a>
        </h3>
      </xsl:if>
      <xsl:apply-templates select="*"/>
    </div>
  </xsl:template>

  <xsl:template match="subsection">
    <div class="h4">
      <xsl:if test="@name">
        <h4>
          <a name="{@name}"><xsl:value-of select="@name"/></a>
        </h4>
      </xsl:if>
      <xsl:apply-templates select="*"/>
    </div>
  </xsl:template>

  <xsl:template match="source">
    <div id="source">
      <pre><xsl:value-of select="."/></pre>
    </div>
  </xsl:template>

  <xsl:template match="footer">
    <tr>
      <td>
        <xsl:apply-templates select="*"/>
      </td>
    </tr>
  </xsl:template>

  <xsl:template match="table">
    <table cellpadding="3" cellspacing="2" border="1" width="100%">
      <xsl:apply-templates select="*"/>
    </table>
  </xsl:template>

  <xsl:template match="tr">
    <xsl:variable name="class">
      <xsl:choose>
        <xsl:when test="(position() mod 2) = 0">a</xsl:when>
        <xsl:otherwise>b</xsl:otherwise>
      </xsl:choose>
    </xsl:variable>

    <tr class="{$class}">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="*"/>
    </tr>
  </xsl:template>

  <!-- copy any other elements through -->
          <xsl:template match = "node()|@*" > 
               <xsl:copy > 
                    <xsl:apply-templates select = "node()|@*" /> 
               </xsl:copy> 
          </xsl:template> 

</xsl:stylesheet>
