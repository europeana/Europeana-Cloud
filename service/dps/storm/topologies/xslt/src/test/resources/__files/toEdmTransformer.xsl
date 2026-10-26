<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="3.0"
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:edm="http://www.europeana.eu/schemas/edm/"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:openarchives="http://www.openarchives.org/OAI/2.0/"
                xmlns:dcterms="http://purl.org/dc/terms/"
                xmlns:ore="http://www.openarchives.org/ore/terms/"
                xmlns:lib="http://example.org/lib"
                xmlns:svcs="http://rdfs.org/sioc/services#"
                xmlns:doap="http://usefulinc.com/ns/doap#"
                xmlns="http://www.openarchives.org/OAI/2.0/">

  <xsl:output method="xml" indent="yes" encoding="utf-8"/>
  <xsl:mode name="text" on-no-match="deep-copy"/>
  <xsl:variable name="uri" select="substring(//openarchives:identifier, 19, 26)"/>

  <xsl:template match="/">

    <rdf:RDF>
      <xsl:copy copy-namespaces="no"/>
      <edm:ProvidedCHO>
        <xsl:attribute name="rdf:about">
          <xsl:text>http://gallica.bnf.fr</xsl:text> <!--Beware, I removed the 's': test if this works!-->
          <xsl:value-of select="$uri"/>
        </xsl:attribute>
        <xsl:apply-templates select="//oai_dc:dc/dc:title" mode="text"/>
        <xsl:apply-templates select="//dc:creator" mode="text"/>
        <xsl:apply-templates select="//dc:contributor" mode="text"/>
        <xsl:apply-templates select="//dc:source" mode="text"/>
        <xsl:apply-templates select="//dc:type" mode="text"/>
        <xsl:apply-templates select="//dc:publisher" mode="text"/>
        <xsl:apply-templates select="//dc:language" mode="text"/>
        <xsl:apply-templates select="//dc:description" xml:lang="fre" mode="text"/>
        <xsl:apply-templates select="//dc:format" mode="text"/>
        <xsl:apply-templates select="//dc:rights" mode="text"/>
        <xsl:apply-templates select="//dc:identifier" mode="text"/>
        <xsl:apply-templates select="//dc:relation" mode="text"/>
        <xsl:apply-templates select="//dc:coverage" mode="text"/>
        <xsl:for-each select="//dc:subject">
          <xsl:element name="dc:subject">
            <xsl:attribute name="xml:lang">
              <xsl:text>fre</xsl:text>
            </xsl:attribute>
            <xsl:value-of select="."/>
          </xsl:element>
        </xsl:for-each>
        <xsl:for-each select="//dc:date">
          <xsl:element name="dcterms:created">
            <xsl:value-of select="."/>
          </xsl:element>
        </xsl:for-each>
        <xsl:element name="dc:subject">
          <xsl:attribute name="rdf:resource">
            <xsl:text>http://vocab.getty.edu/aat/300020756</xsl:text>
          </xsl:attribute>
        </xsl:element>
        <xsl:element name="dc:subject">
          <xsl:attribute name="rdf:resource">
            <xsl:text>http://vocab.getty.edu/aat/300411614</xsl:text>
          </xsl:attribute>
        </xsl:element>
        <xsl:element name="dc:subject">
          <xsl:attribute name="rdf:resource">
            <xsl:text>http://vocab.getty.edu/aat/300411614</xsl:text>
          </xsl:attribute>
        </xsl:element>
        <xsl:element name="dc:type">
          <xsl:attribute name="rdf:resource">
            <xsl:text>http://vocab.getty.edu/aat/300028569</xsl:text>
          </xsl:attribute>
        </xsl:element>
        <edm:type>TEXT</edm:type>
        <dc:rights>
          <xsl:attribute name="rdf:resource">
            <xsl:text>https://rightsstatements.org/page/NoC-OKLR/1.0/?relatedURL=https://gallica.bnf.fr/edit/und/conditions-dutilisation-des-contenus-de-gallica</xsl:text>
          </xsl:attribute>
        </dc:rights>
        <xsl:element name="dcterms:isPartOf">
          <xsl:text>Art of Reading in the Middle Ages: updated item</xsl:text>
        </xsl:element>
      </edm:ProvidedCHO>
      <ore:Aggregation>
        <xsl:attribute name="rdf:about" select="concat($uri,'#agg')"/>
        <edm:aggregatedCHO>
          <xsl:attribute name="rdf:resource">
            <xsl:text>https://gallica.bnf.fr</xsl:text>
            <xsl:value-of select="$uri"/>
          </xsl:attribute>
        </edm:aggregatedCHO>
        <xsl:element name="edm:dataProvider">
          <xsl:text>Bibliothèque nationale de France</xsl:text>
        </xsl:element>
        <xsl:element name="edm:isShownAt">
          <xsl:attribute name="rdf:resource">
            <xsl:text>https://gallica.bnf.fr</xsl:text>
            <xsl:value-of select="$uri"/>
          </xsl:attribute>
        </xsl:element>
        <xsl:element name="edm:isShownBy">
          <xsl:attribute name="rdf:resource">
            <xsl:text>https://gallica.bnf.fr/iiif</xsl:text>
            <xsl:value-of select="$uri"/>
            <xsl:text>/f1/full/full/0/native.jpg</xsl:text>
          </xsl:attribute>
        </xsl:element>
        <xsl:element name="edm:object">
          <xsl:attribute name="rdf:resource">
            <xsl:text>https://gallica.bnf.fr/iiif</xsl:text>
            <xsl:value-of select="$uri"/>
            <xsl:text>/f1/full/512,/0/native.jpg</xsl:text>
          </xsl:attribute>
        </xsl:element>
        <edm:provider>
          <xsl:text>Bibliothèque nationale de France</xsl:text>
        </edm:provider>
        <edm:rights>
          <xsl:attribute name="rdf:resource">
            <xsl:text>http://rightsstatements.org/vocab/NoC-OKLR/1.0/</xsl:text>
          </xsl:attribute>
        </edm:rights>
        <dc:rights>
          <xsl:attribute name="rdf:resource">
            <xsl:text>https://rightsstatements.org/page/NoC-OKLR/1.0/?relatedURL=https://gallica.bnf.fr/edit/und/conditions-dutilisation-des-contenus-de-gallica</xsl:text>
          </xsl:attribute>
        </dc:rights>
      </ore:Aggregation>
      <edm:WebResource>
        <xsl:attribute name="rdf:about">
          <xsl:text>https://gallica.bnf.fr/iiif</xsl:text>
          <xsl:value-of select="$uri"/>
          <xsl:text>/f1/full/full/0/native.jpg</xsl:text>
        </xsl:attribute>
        <dc:format>
          <xsl:text>jpg</xsl:text>
        </dc:format>
        <dcterms:isReferencedBy>
          <xsl:attribute name="rdf:resource">
            <xsl:text>https://gallica.bnf.fr/iiif</xsl:text>
            <xsl:value-of select="$uri"/>
            <xsl:text>/manifest.json</xsl:text>
          </xsl:attribute>
        </dcterms:isReferencedBy>
        <svcs:has_service>
          <xsl:attribute name="rdf:resource">
            <xsl:text>http://gallica.bnf.fr/iiif</xsl:text>
            <xsl:value-of select="$uri"/>
            <xsl:text>/f1</xsl:text>
          </xsl:attribute>
        </svcs:has_service>
      </edm:WebResource>
      <svcs:Service>
        <xsl:attribute name="rdf:about">
          <xsl:text>http://gallica.bnf.fr/iiif</xsl:text>
          <xsl:value-of select="$uri"/>
          <xsl:text>/f1</xsl:text>
        </xsl:attribute>
        <dcterms:conformsTo>
          <xsl:attribute name="rdf:resource">
            <xsl:text>http://iiif.io/api/image</xsl:text>
          </xsl:attribute>
        </dcterms:conformsTo>
        <doap:implements>
          <xsl:attribute name="rdf:resource">
            <xsl:text>http://iiif.io/api/image/2/level2.json</xsl:text>
          </xsl:attribute>
        </doap:implements>
      </svcs:Service>

    </rdf:RDF>

  </xsl:template>
</xsl:stylesheet>