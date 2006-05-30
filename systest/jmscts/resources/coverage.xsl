<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0"
                xmlns:c="http://jmscts.sourceforge.net/coverage"
                xmlns:t="http://jmscts.sourceforge.net/test">

  <xsl:include href="coverage-requirements.xsl"/>
  <xsl:include href="coverage-tests.xsl"/>

  <xsl:output method="xml" indent="yes"/>

  <xsl:template match="/">
    <document>
      <body>
        <xsl:apply-templates select="c:requirementCoverage"/>
      </body>
    </document>
  </xsl:template>

  <xsl:template match="c:requirementCoverage">
    <section name="Overview">
      <xsl:call-template name="requirements-summary"/>
      <xsl:call-template name="tests-summary"/>
    </section>

    <section name="Requirements">
      <xsl:call-template name="requirements-passed"/>
      <xsl:call-template name="requirements-failed"/>
      <xsl:call-template name="requirements-untested"/>
    </section>

    <section name="Tests">
      <xsl:call-template name="tests-passed"/>
      <xsl:call-template name="tests-failed"/>
    </section>

  </xsl:template>

  <xsl:template name="requirements-summary">
    <subsection name="Requirements summary">
      <p>
        <table>
          <tr>
            <td><a href="#Passed">Passed</a></td>
            <td><xsl:call-template name="get-requirements-passed"/></td>
          </tr>
          <tr>
            <td><a href="#Failed">Failed</a></td>
            <td><xsl:call-template name="get-requirements-failed"/></td>
          </tr>
          <tr>
            <td><a href="#Untested">Untested</a></td>
            <td><xsl:call-template name="get-requirements-untested"/></td>
          </tr>
          <tr>
            <td>Total</td>
            <td><xsl:call-template name="get-requirements"/></td>
          </tr>
        </table>
      </p>
    </subsection>
  </xsl:template>

  <xsl:template name="tests-summary">
    <subsection name="Tests summary">
      <p>
        <table>
          <tr>
            <td><a href="#Test Cases: Passed">Passed</a></td>
            <td><xsl:call-template name="get-tests-passed"/></td>
          </tr>
          <tr>
            <td><a href="#Test Cases: Failed">Failed</a></td>
            <td><xsl:call-template name="get-tests-failed"/></td>
          </tr>
          <tr>
            <td>Total</td>
            <td><xsl:call-template name="get-tests"/></td>
          </tr>
        </table>
      </p>
    </subsection>
  </xsl:template>

  <xsl:template name="requirements-passed">
    <subsection name="Passed">
      <p>
        <table>
          <tr>
            <th>Requirement</th>
            <th>Tests</th>
          </tr>
          <xsl:apply-templates select="c:coverage[test and @failures = 0]"
                               mode="passed">
            <xsl:sort select="requirementId"/>
          </xsl:apply-templates>
        </table>
      </p>
    </subsection>
  </xsl:template>

  <xsl:template name="requirements-failed">
    <subsection name="Failed">
      <p>
        <table>
          <tr>
            <th>Requirement</th>
            <th>Tests</th>
            <th>Failures</th>
          </tr>
          <xsl:apply-templates select="c:coverage[@failures != 0]"
                               mode="failed">
            <xsl:sort select="requirementId"/>
          </xsl:apply-templates>
        </table>
      </p>
    </subsection>
  </xsl:template>

  <xsl:template name="requirements-untested">
    <subsection name="Untested">
      <p>
        <table>
          <tr><th>Requirement</th></tr>
          <xsl:apply-templates select="c:coverage[count(test) = 0]"
                               mode="untested">
            <xsl:sort select="requirementId"/>
          </xsl:apply-templates>
        </table>
      </p>
    </subsection>
  </xsl:template>

  <xsl:template name="tests-passed">
    <subsection name="Test Cases: Passed">
      <p>
        <table>
          <tr>
            <th>Test Case</th>
            <th>Tests</th>
          </tr>
          <xsl:apply-templates 
               select="t:testRuns[count(t:testRun/t:failure) = 0]"
                                  mode="passed">
            <xsl:sort select="test"/>
          </xsl:apply-templates>
        </table>
      </p>
    </subsection>
  </xsl:template>

  <xsl:template name="tests-failed">
    <subsection name="Test Cases: Failed">
      <p>
        <table>
          <tr>
            <th>Test Case</th>
            <th>Tests</th>
            <th>Failures</th>
          </tr>
          <xsl:apply-templates 
               select="t:testRuns[count(t:testRun/t:failure) != 0]"
                                  mode="failed">
            <xsl:sort select="test"/>
          </xsl:apply-templates>
        </table>
      </p>
    </subsection>
  </xsl:template>

  <xsl:template match="t:testRuns" mode="passed">
    <tr>
      <td>
        <a href="{test}.html"><small><xsl:value-of select="test"/></small></a>
      </td>
      <td><small><xsl:value-of select="count(t:testRun)"/></small></td>      
    </tr>
    <xsl:call-template name="output-test"/>
  </xsl:template>

  <xsl:template match="t:testRuns" mode="failed">
    <tr>
      <td>
        <a href="{test}.html"><small><xsl:value-of select="test"/></small></a>
      </td>
      <td><small><xsl:value-of select="count(t:testRun)"/></small></td>
      <td>
        <small>
          <xsl:value-of select="count(t:testRun/t:failure)"/>
        </small>
      </td>
    </tr>
    <xsl:call-template name="output-test"/>
  </xsl:template>

  <xsl:template match="c:coverage" mode="passed">
    <tr>
      <xsl:call-template name="print-requirement"/>
      <td><small><xsl:value-of select="@runs"/></small></td>
    </tr>
    <xsl:call-template name="output-requirement"/>
  </xsl:template>

  <xsl:template match="c:coverage" mode="failed">
    <tr>
      <xsl:call-template name="print-requirement"/>
      <td><small><xsl:value-of select="@runs"/></small></td>
      <td>        
        <a href="{requirementId}.html">
          <small><xsl:value-of select="@failures"/></small>
        </a>
      </td>
    </tr>
    <xsl:call-template name="output-requirement"/>
  </xsl:template>

  <xsl:template match="c:coverage" mode="untested">
    <tr>
      <xsl:call-template name="print-requirement"/>
    </tr>
    <xsl:call-template name="output-requirement"/>
  </xsl:template>

  <xsl:template name="print-requirement">
    <xsl:variable name="reqId" select="requirementId"/>
    <td>
      <small>
        <a href="{$reqId}.html"><xsl:value-of select="$reqId"/></a>
      </small>
    </td>
  </xsl:template>

  <xsl:template name="get-requirements">          
    <xsl:value-of select="count(c:coverage)"/>
  </xsl:template>

  <xsl:template name="get-requirements-passed">
    <xsl:value-of select="count(c:coverage[@runs != 0 and @failures = 0])"/>
  </xsl:template>

  <xsl:template name="get-requirements-failed">
    <xsl:value-of select="count(c:coverage[@failures != 0])"/>
  </xsl:template>

  <xsl:template name="get-requirements-untested">
    <xsl:value-of select="count(c:coverage[@runs = 0])"/>
  </xsl:template>

  <xsl:template name="get-tests">
    <xsl:value-of select="count(t:testRuns/t:testRun)"/>
  </xsl:template>

  <xsl:template name="get-tests-passed">
    <xsl:variable name="total">
      <xsl:call-template name="get-tests"/>
    </xsl:variable>
    <xsl:variable name="failed">
      <xsl:call-template name="get-tests-failed"/>
    </xsl:variable>
    <xsl:value-of select="$total - $failed"/>
  </xsl:template>

  <xsl:template name="get-tests-failed">
    <xsl:value-of select="count(t:testRuns/t:testRun/t:failure)"/>
  </xsl:template>

</xsl:stylesheet>
