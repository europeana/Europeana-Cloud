<xsl:stylesheet version="2.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  xmlns:edm="http://www.europeana.eu/schemas/edm/">
  <xsl:param name="providedCHOAboutId"/>
  <xsl:template match="rdf:RDF">
    <rdf:RDF>
      <xsl:call-template   name="ProvidedCHO"/>
    </rdf:RDF>
  </xsl:template>
  <xsl:template name="ProvidedCHO">
    <xsl:element name="edm:ProvidedCHO">
      <xsl:attribute name="rdf:about" select="$providedCHOAboutId"/>
    </xsl:element>
  </xsl:template>
</xsl:stylesheet>