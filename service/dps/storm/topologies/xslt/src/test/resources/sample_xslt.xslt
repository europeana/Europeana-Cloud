<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0"
                xmlns:adms="http://www.w3.org/ns/adms#"
                xmlns:cc="http://creativecommons.org/ns#"
                xmlns:crm="http://www.cidoc-crm.org/rdfs/cidoc_crm_v5.0.2_english_label.rdfs#"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:dcat="http://www.w3.org/ns/dcat#"
                xmlns:dcterms="http://purl.org/dc/terms/"
                xmlns:doap="http://usefulinc.com/ns/doap#"
                xmlns:ebucore="http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#"
                xmlns:edm="http://www.europeana.eu/schemas/edm/"
                xmlns:foaf="http://xmlns.com/foaf/0.1/"
                xmlns:odrl="http://www.w3.org/ns/odrl/2/"
                xmlns:ore="http://www.openarchives.org/ore/terms/"
                xmlns:owl="http://www.w3.org/2002/07/owl#"
                xmlns:rdaGr2="http://rdvocab.info/ElementsGr2/"
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
                xmlns:skos="http://www.w3.org/2004/02/skos/core#"
                xmlns:svcs="http://rdfs.org/sioc/services#"
                xmlns:wgs84_pos="http://www.w3.org/2003/01/geo/wgs84_pos#"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xs="http://www.w3.org/2001/XMLSchema"
                xmlns:f="functions-namespace"

                exclude-result-prefixes="xsi xsl f">
  <xsl:output omit-xml-declaration="no" indent="yes"/>
  <!-- ************************************************ -->
  <!--                  INPUT VARIABLES                 -->
  <!-- ************************************************ -->
  <xsl:param name="datasetName" select="'exampleName'"/>
  <xsl:param name="edmCountry"   select="'France'"/>
  <xsl:param name="edmLanguage"  select="'fr'"    />
  <xsl:param name="providedCHOAboutId" select="'/00000/some_record_id1'"/>
  <!-- ************************************************ -->
  <!--                   CONSTANTS                      -->
  <!-- ************************************************ -->
  <!-- Fixed strings to mint URIs -->
  <xsl:variable name="europeana"       select="'Europeana Foundation'" />
  <!--
  <xsl:variable name="provider_uri"    select="/rdf:RDF/edm:ProvidedCHO[1]/@rdf:about" /><xsl:variable name="uri_cho"         select="f:mintUri($prefix_cho   , $datasetId, $provider_uri)" /><xsl:variable name="uri_dproxy"      select="f:mintUri($prefix_dproxy, $datasetId, $provider_uri)" /><xsl:variable name="uri_pproxy"      select="f:mintUri($prefix_pproxy, $datasetId, $provider_uri)" /><xsl:variable name="uri_eproxy"      select="f:mintUri($prefix_eproxy, $datasetId, $provider_uri)" /><xsl:variable name="uri_daggr"       select="f:mintUri($prefix_daggr , $datasetId, $provider_uri)" /><xsl:variable name="uri_paggr"       select="f:mintUri($prefix_paggr , $datasetId, $provider_uri)" /><xsl:variable name="uri_eaggr"       select="f:mintUri($prefix_eaggr , $datasetId, $provider_uri)" />
  -->
  <xsl:variable name="uri_cho"         select="$providedCHOAboutId" />
  <xsl:variable name="uri_dproxy"      select="concat('/proxy/provider', $providedCHOAboutId)" />
  <xsl:variable name="uri_pproxy"      select="concat('/proxy/aggregator', $providedCHOAboutId)" />
  <xsl:variable name="uri_eproxy"      select="concat('/proxy/europeana', $providedCHOAboutId)" />
  <xsl:variable name="uri_daggr"       select="concat('/aggregation/provider', $providedCHOAboutId)" />
  <xsl:variable name="uri_paggr"       select="concat('/aggregation/aggregator', $providedCHOAboutId)" />
  <xsl:variable name="uri_eaggr"       select="concat('/aggregation/europeana', $providedCHOAboutId)" />
  <xsl:variable name="provenance"       select="exists(/rdf:RDF/edm:ProvidedCHO/*/@edm:wasGeneratedBy)" />
  <!-- ************************************************ -->
  <!--                   TEMPLATES                      -->
  <!-- ************************************************ -->
  <xsl:template match="/">
    <xsl:apply-templates select="*"/>
  </xsl:template>
  <xsl:template match="rdf:RDF">
    <rdf:RDF>
      <xsl:call-template   name="ProvidedCHO"/>
      <xsl:apply-templates select="edm:WebResource"/>
      <xsl:apply-templates select="edm:Agent"/>
      <xsl:apply-templates select="edm:Place"/>
      <xsl:apply-templates select="edm:TimeSpan"/>
      <xsl:apply-templates select="skos:Concept"/>
      <xsl:apply-templates select="ore:Aggregation"/>
      <xsl:apply-templates select="edm:ProvidedCHO"/>
      <xsl:call-template   name="EuropeanaAggregation"/>
      <xsl:apply-templates select="cc:License"/>
      <xsl:apply-templates select="foaf:Organization"/>
      <xsl:apply-templates select="dcat:Dataset"/>
      <xsl:apply-templates select="svcs:Service"/>
    </rdf:RDF>
  </xsl:template>
  <xsl:template name="ProvidedCHO">
    <xsl:element name="edm:ProvidedCHO">
      <xsl:attribute name="rdf:about" select="$uri_cho"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="ore:Aggregation">
    <!-- Data Provider Aggregation -->
    <xsl:element name="ore:Aggregation">
      <xsl:attribute name="rdf:about" select="$uri_daggr" />
      <xsl:element name="edm:aggregatedCHO">
        <xsl:attribute name="rdf:resource" select="$uri_cho" />
      </xsl:element>
      <xsl:element name="edm:dataProvider">
        <xsl:copy-of select="edm:dataProvider/@*"/>
        <xsl:copy-of select="edm:dataProvider/text()"/>
      </xsl:element>
      <xsl:apply-templates select="edm:hasView"/>
      <xsl:apply-templates select="edm:isShownAt"/>
      <xsl:apply-templates select="edm:isShownBy"/>
      <xsl:apply-templates select="edm:object"/>
      <xsl:element name="edm:provider">
        <xsl:copy-of select="edm:provider/@*"/>
        <xsl:copy-of select="edm:provider/text()"/>
      </xsl:element>
      <xsl:apply-templates select="dc:rights"/>
      <xsl:apply-templates select="edm:rights"/>
      <xsl:apply-templates select="edm:ugc"/>
      <xsl:apply-templates select="edm:intermediateProvider"/>
    </xsl:element>
    <!-- Provider (ie. Aggregator) Aggregation -->
    <xsl:if test="$provenance">
      <xsl:element name="ore:Aggregation">
        <xsl:attribute name="rdf:about" select="$uri_paggr" />
        <xsl:element name="edm:aggregatedCHO">
          <xsl:attribute name="rdf:resource" select="$uri_cho" />
        </xsl:element>
        <xsl:element name="edm:dataProvider">
          <xsl:copy-of select="edm:provider/@*"/>
          <xsl:copy-of select="edm:provider/text()"/>
        </xsl:element>
        <xsl:apply-templates select="edm:isShownAt"/>
        <xsl:apply-templates select="edm:isShownBy"/>
        <xsl:element name="edm:provider">
          <xsl:copy-of select="edm:provider/@*"/>
          <xsl:copy-of select="edm:provider/text()"/>
        </xsl:element>
        <xsl:apply-templates select="edm:rights"/>
      </xsl:element>
    </xsl:if>
  </xsl:template>
  <xsl:template name="EuropeanaAggregation">
    <xsl:element name="edm:EuropeanaAggregation">
      <xsl:attribute name="rdf:about" select="$uri_eaggr" />
      <xsl:element name="edm:aggregatedCHO">
        <xsl:attribute name="rdf:resource" select="$uri_cho" />
      </xsl:element>
      <edm:dataProvider xml:lang="en">
        <xsl:value-of select="$europeana"/>
      </edm:dataProvider>
      <edm:provider xml:lang="en">
        <xsl:value-of select="$europeana"/>
      </edm:provider>
      <xsl:element name="edm:datasetName">
        <xsl:value-of select="$datasetName" />
      </xsl:element>
      <xsl:element name="edm:country">
        <xsl:value-of select="$edmCountry" />
      </xsl:element>
      <xsl:element name="edm:language">
        <xsl:value-of select="$edmLanguage" />
      </xsl:element>
    </xsl:element>
  </xsl:template>
  <xsl:template match="edm:ProvidedCHO">
    <xsl:call-template name="Proxy">
      <xsl:with-param name="dp" select="true()"/>
    </xsl:call-template>
    <xsl:if test="$provenance">
      <xsl:call-template name="Proxy">
        <xsl:with-param name="dp" select="false()"/>
      </xsl:call-template>
    </xsl:if>
    <xsl:call-template name="EuropeanaProxy"/>
  </xsl:template>
  <xsl:template name="Proxy">
    <xsl:param name="dp"/>
    <xsl:element name="ore:Proxy">
      <xsl:if test="$dp">
        <xsl:attribute name="rdf:about" select="$uri_dproxy" />
      </xsl:if>
      <xsl:if test="not($dp)">
        <xsl:attribute name="rdf:about" select="$uri_pproxy" />
      </xsl:if>
      <xsl:apply-templates select="dc:contributor[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:coverage[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:creator[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:date[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:description[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:format[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:identifier[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:language[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:publisher[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:relation[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:rights[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:source[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:subject[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:title[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dc:type[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:alternative[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:conformsTo[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:created[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:extent[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:hasFormat[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:hasPart[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:hasVersion[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:isFormatOf[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:isPartOf[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:isReferencedBy[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:isReplacedBy[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:isRequiredBy[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:issued[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:isVersionOf[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:medium[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:provenance[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:references[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:replaces[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:requires[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:spatial[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:tableOfContents[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="dcterms:temporal[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:currentLocation[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:hasMet[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:hasType[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:incorporates[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:isDerivativeOf[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:isNextInSequence[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:isRelatedTo[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:isRepresentationOf[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:isSimilarTo[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:isSuccessorOf[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:realizes[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <xsl:apply-templates select="edm:userTag[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
      <edm:europeanaProxy>false</edm:europeanaProxy>
      <xsl:element name="ore:proxyFor">
        <xsl:attribute name="rdf:resource" select="$uri_cho" />
      </xsl:element>
      <xsl:element name="ore:proxyIn">
        <xsl:if test="$dp">
          <xsl:attribute name="rdf:resource" select="$uri_daggr" />
        </xsl:if>
        <xsl:if test="not($dp)">
          <xsl:attribute name="rdf:resource" select="$uri_paggr" />
        </xsl:if>
      </xsl:element>
      <xsl:if test="not($dp)">
        <ore:lineage rdf:resource="{$uri_dproxy}"/>
      </xsl:if>
      <xsl:apply-templates select="edm:type"/>
      <xsl:apply-templates select="owl:sameAs[f:xor($dp,exists(@edm:wasGeneratedBy))]"/>
    </xsl:element>
  </xsl:template>
  <xsl:template name="EuropeanaProxy">
    <xsl:element name="ore:Proxy">
      <xsl:attribute name="rdf:about" select="$uri_eproxy" />
      <dc:identifier>
        <xsl:value-of select="@rdf:about"/>
      </dc:identifier>
      <edm:europeanaProxy>true</edm:europeanaProxy>
      <xsl:element name="ore:proxyFor">
        <xsl:attribute name="rdf:resource" select="$uri_cho" />
      </xsl:element>
      <xsl:element name="ore:proxyIn">
        <xsl:attribute name="rdf:resource" select="$uri_eaggr" />
      </xsl:element>
      <ore:lineage rdf:resource="{$uri_dproxy}"/>
      <xsl:if test="$provenance">
        <ore:lineage rdf:resource="{$uri_pproxy}"/>
      </xsl:if>
    </xsl:element>
  </xsl:template>
  <xsl:template match="edm:WebResource">
    <xsl:element name="edm:WebResource">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="dc:creator"/>
      <xsl:apply-templates select="dc:description"/>
      <xsl:apply-templates select="dc:format"/>
      <xsl:apply-templates select="dc:rights"/>
      <xsl:apply-templates select="dc:source"/>
      <xsl:apply-templates select="dc:type"/>
      <xsl:apply-templates select="dcterms:conformsTo"/>
      <xsl:apply-templates select="dcterms:created"/>
      <xsl:apply-templates select="dcterms:extent"/>
      <xsl:apply-templates select="dcterms:hasPart"/>
      <xsl:apply-templates select="dcterms:isFormatOf"/>
      <xsl:apply-templates select="dcterms:isPartOf"/>
      <xsl:apply-templates select="dcterms:issued"/>
      <xsl:apply-templates select="edm:isNextInSequence"/>
      <xsl:apply-templates select="edm:rights"/>
      <xsl:apply-templates select="owl:sameAs"/>
      <xsl:apply-templates select="rdf:type"/>
      <xsl:apply-templates select="edm:codecName"/>
      <xsl:apply-templates select="ebucore:hasMimeType"/>
      <xsl:apply-templates select="ebucore:fileByteSize"/>
      <xsl:apply-templates select="ebucore:duration"/>
      <xsl:apply-templates select="ebucore:width"/>
      <xsl:apply-templates select="ebucore:height"/>
      <xsl:apply-templates select="edm:spatialResolution"/>
      <xsl:apply-templates select="ebucore:sampleSize"/>
      <xsl:apply-templates select="ebucore:sampleRate"/>
      <xsl:apply-templates select="ebucore:bitRate"/>
      <xsl:apply-templates select="ebucore:frameRate"/>
      <xsl:apply-templates select="edm:hasColorSpace"/>
      <xsl:apply-templates select="edm:componentColor"/>
      <xsl:apply-templates select="ebucore:orientation"/>
      <xsl:apply-templates select="ebucore:audioChannelNumber"/>
      <xsl:apply-templates select="dcterms:isReferencedBy"/>
      <xsl:apply-templates select="edm:preview"/>
      <xsl:apply-templates select="svcs:has_service"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="edm:Agent">
    <xsl:element name="edm:Agent">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="skos:prefLabel"/>
      <xsl:apply-templates select="skos:altLabel"/>
      <xsl:apply-templates select="skos:note"/>
      <xsl:apply-templates select="dc:date"/>
      <xsl:apply-templates select="dc:identifier"/>
      <xsl:apply-templates select="dcterms:hasPart"/>
      <xsl:apply-templates select="dcterms:isPartOf"/>
      <xsl:apply-templates select="edm:begin"/>
      <xsl:apply-templates select="edm:end"/>
      <xsl:apply-templates select="edm:hasMet"/>
      <xsl:apply-templates select="edm:isRelatedTo"/>
      <xsl:apply-templates select="foaf:name"/>
      <xsl:apply-templates select="rdaGr2:biographicalInformation"/>
      <xsl:apply-templates select="rdaGr2:dateOfBirth"/>
      <xsl:apply-templates select="rdaGr2:dateOfDeath"/>
      <xsl:apply-templates select="rdaGr2:dateOfEstablishment"/>
      <xsl:apply-templates select="rdaGr2:dateOfTermination"/>
      <xsl:apply-templates select="rdaGr2:gender"/>
      <xsl:apply-templates select="rdaGr2:placeOfBirth"/>
      <xsl:apply-templates select="rdaGr2:placeOfDeath"/>
      <xsl:apply-templates select="rdaGr2:professionOrOccupation"/>
      <xsl:apply-templates select="owl:sameAs"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="edm:Place">
    <xsl:element name="edm:Place">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="wgs84_pos:lat"/>
      <xsl:apply-templates select="wgs84_pos:long"/>
      <xsl:apply-templates select="wgs84_pos:alt"/>
      <xsl:apply-templates select="skos:prefLabel"/>
      <xsl:apply-templates select="skos:altLabel"/>
      <xsl:apply-templates select="skos:note"/>
      <xsl:apply-templates select="dcterms:hasPart"/>
      <xsl:apply-templates select="dcterms:isPartOf"/>
      <xsl:apply-templates select="edm:isNextInSequence"/>
      <xsl:apply-templates select="owl:sameAs"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="edm:TimeSpan">
    <xsl:element name="edm:TimeSpan">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="skos:prefLabel"/>
      <xsl:apply-templates select="skos:altLabel"/>
      <xsl:apply-templates select="skos:note"/>
      <xsl:apply-templates select="dcterms:hasPart"/>
      <xsl:apply-templates select="dcterms:isPartOf"/>
      <xsl:apply-templates select="edm:begin"/>
      <xsl:apply-templates select="edm:end"/>
      <xsl:apply-templates select="edm:isNextInSequence"/>
      <xsl:apply-templates select="owl:sameAs"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="skos:Concept">
    <xsl:element name="skos:Concept">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="skos:prefLabel"/>
      <xsl:apply-templates select="skos:altLabel"/>
      <xsl:apply-templates select="skos:broader"/>
      <xsl:apply-templates select="skos:narrower"/>
      <xsl:apply-templates select="skos:related"/>
      <xsl:apply-templates select="skos:broadMatch"/>
      <xsl:apply-templates select="skos:narrowMatch"/>
      <xsl:apply-templates select="skos:relatedMatch"/>
      <xsl:apply-templates select="skos:exactMatch"/>
      <xsl:apply-templates select="skos:closeMatch"/>
      <xsl:apply-templates select="skos:note"/>
      <xsl:apply-templates select="skos:notation"/>
      <xsl:apply-templates select="skos:inScheme"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="cc:License">
    <xsl:element name="cc:License">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="odrl:inheritFrom"/>
      <xsl:apply-templates select="cc:deprecatedOn"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="foaf:Organization">
    <xsl:element name="foaf:Organization">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="edm:acronym"/>
      <xsl:apply-templates select="edm:organizationScope"/>
      <xsl:apply-templates select="edm:organizationDomain"/>
      <xsl:apply-templates select="edm:organizationSector"/>
      <xsl:apply-templates select="edm:geographicLevel"/>
      <xsl:apply-templates select="edm:country"/>
      <xsl:apply-templates select="edm:europeanaRole"/>
      <xsl:apply-templates select="foaf:homepage"/>
      <xsl:apply-templates select="foaf:logo"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="dcat:Dataset">
    <xsl:element name="dcat:Dataset">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="edm:datasetName"/>
      <xsl:apply-templates select="edm:provider"/>
      <xsl:apply-templates select="edm:intermediateProvider"/>
      <xsl:apply-templates select="edm:dataProvider"/>
      <xsl:apply-templates select="edm:country"/>
      <xsl:apply-templates select="edm:language"/>
      <xsl:apply-templates select="dc:identifier"/>
      <xsl:apply-templates select="dc:description"/>
      <xsl:apply-templates select="dcterms:created"/>
      <xsl:apply-templates select="dcterms:extent"/>
      <xsl:apply-templates select="dcterms:modified"/>
      <xsl:apply-templates select="adms:status"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="svcs:Service">
    <xsl:element name="svcs:Service">
      <xsl:copy-of select="@*"/>
      <xsl:apply-templates select="dcterms:conformsTo"/>
      <xsl:apply-templates select="doap:implements"/>
    </xsl:element>
  </xsl:template>
  <xsl:template match="text()">
    <xsl:copy/>
  </xsl:template>
  <xsl:template match="*">
    <xsl:element name="{name()}">
      <xsl:copy-of select="@rdf:resource"/>
      <xsl:copy-of select="@xml:lang"/>
      <xsl:copy-of select="@rdf:datatype"/>
      <xsl:apply-templates select="node()"/>
    </xsl:element>
  </xsl:template>
  <!-- ************************************************ -->
  <!--                   FUNCTIONS                      -->
  <!-- ************************************************ -->
  <xsl:function name="f:xor">
    <xsl:param name="pX" as="xs:boolean"/>
    <xsl:param name="pY" as="xs:boolean"/>
    <xsl:sequence select="($pX and not($pY)) or ($pY and not($pX))"/>
  </xsl:function>
  <!-- Function replacing all characters but [a to z, A to Z and _] by _ -->
  <xsl:function name="f:normalize">
    <xsl:param name="string" />
    <xsl:value-of select="replace($string, '[^a-zA-Z0-9_]', '_')" />
  </xsl:function>
  <!-- Function for conditional normalizing -->
  <xsl:function name="f:mintUri">
    <!-- Input parameter -->
    <xsl:param name="prefix" />
    <xsl:param name="dataset"/>
    <xsl:param name="uri"    />
    <xsl:variable name="id">
      <xsl:choose>
        <!-- Normalize & removes 1st instance of 'http://' or 'https://' or both former options preceded by 1 or more '#' -->
        <xsl:when test="matches($uri, '^#*https?://')">
          <xsl:value-of select="f:normalize(replace($uri, '^#*https?://[^/]+/', ''))" />
        </xsl:when>
        <!-- Normalize & removes only 1st instance of group of 1 or more '#' -->
        <xsl:when test="matches($uri, '^#+', '')" >
          <xsl:value-of select="f:normalize(replace($uri, '^#+', ''))" />
        </xsl:when>
        <!-- If none of previous conditions, simply normalize the value as is -->
        <xsl:otherwise>
          <xsl:value-of select="f:normalize($uri)" />
        </xsl:otherwise>
      </xsl:choose>
    </xsl:variable>
    <xsl:value-of select="concat($prefix, $dataset, '/', $id)" />
  </xsl:function>
</xsl:stylesheet>
