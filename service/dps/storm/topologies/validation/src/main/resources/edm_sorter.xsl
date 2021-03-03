<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0"
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
	xmlns:adms="http://www.w3.org/ns/adms#"

	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"

    exclude-result-prefixes="xsi xsl">

    <xsl:output omit-xml-declaration="no" indent="yes"/>

	<xsl:template match="/">
		<xsl:apply-templates select="*"/>
	</xsl:template>

	<xsl:template match="rdf:RDF">
		<rdf:RDF>
			<xsl:apply-templates select="edm:ProvidedCHO"/>
			<xsl:apply-templates select="edm:WebResource"/>
			<xsl:apply-templates select="edm:Agent"/>
			<xsl:apply-templates select="edm:Place"/>
			<xsl:apply-templates select="edm:TimeSpan"/>
			<xsl:apply-templates select="skos:Concept"/>
			<xsl:apply-templates select="ore:Aggregation"/>
			<xsl:apply-templates select="ore:Proxy"/>
			<xsl:apply-templates select="edm:EuropeanaAggregation"/>
			<xsl:apply-templates select="cc:License"/>
			<xsl:apply-templates select="foaf:Organization"/>
			<xsl:apply-templates select="dcat:Dataset"/>
			<xsl:apply-templates select="svcs:Service"/>
		</rdf:RDF>
	</xsl:template>

	<xsl:template match="edm:ProvidedCHO">
		<xsl:element name="edm:ProvidedCHO">
			<xsl:copy-of select="@*"/>
			<xsl:apply-templates select="dc:contributor"/>
			<xsl:apply-templates select="dc:coverage"/>
			<xsl:apply-templates select="dc:creator"/>
			<xsl:apply-templates select="dc:date"/>
			<xsl:apply-templates select="dc:description"/>
			<xsl:apply-templates select="dc:format"/>
			<xsl:apply-templates select="dc:identifier"/>
			<xsl:apply-templates select="dc:language"/>
			<xsl:apply-templates select="dc:publisher"/>
			<xsl:apply-templates select="dc:relation"/>
			<xsl:apply-templates select="dc:rights"/>
			<xsl:apply-templates select="dc:source"/>
			<xsl:apply-templates select="dc:subject"/>
			<xsl:apply-templates select="dc:title"/>
			<xsl:apply-templates select="dc:type"/>
			<xsl:apply-templates select="dcterms:alternative"/>
			<xsl:apply-templates select="dcterms:conformsTo"/>
			<xsl:apply-templates select="dcterms:created"/>
			<xsl:apply-templates select="dcterms:extent"/>
			<xsl:apply-templates select="dcterms:hasFormat"/>
			<xsl:apply-templates select="dcterms:hasPart"/>
			<xsl:apply-templates select="dcterms:hasVersion"/>
			<xsl:apply-templates select="dcterms:isFormatOf"/>
			<xsl:apply-templates select="dcterms:isPartOf"/>
			<xsl:apply-templates select="dcterms:isReferencedBy"/>
			<xsl:apply-templates select="dcterms:isReplacedBy"/>
			<xsl:apply-templates select="dcterms:isRequiredBy"/>
			<xsl:apply-templates select="dcterms:issued"/>
			<xsl:apply-templates select="dcterms:isVersionOf"/>
			<xsl:apply-templates select="dcterms:medium"/>
			<xsl:apply-templates select="dcterms:provenance"/>
			<xsl:apply-templates select="dcterms:references"/>
			<xsl:apply-templates select="dcterms:replaces"/>
			<xsl:apply-templates select="dcterms:requires"/>
			<xsl:apply-templates select="dcterms:spatial"/>
			<xsl:apply-templates select="dcterms:tableOfContents"/>
			<xsl:apply-templates select="dcterms:temporal"/>
			<xsl:apply-templates select="edm:currentLocation"/>
			<xsl:apply-templates select="edm:hasMet"/>
			<xsl:apply-templates select="edm:hasType"/>
			<xsl:apply-templates select="edm:incorporates"/>
			<xsl:apply-templates select="edm:isDerivativeOf"/>
			<xsl:apply-templates select="edm:isNextInSequence"/>
			<xsl:apply-templates select="edm:isRelatedTo"/>
			<xsl:apply-templates select="edm:isRepresentationOf"/>
			<xsl:apply-templates select="edm:isSimilarTo"/>
			<xsl:apply-templates select="edm:isSuccessorOf"/>
			<xsl:apply-templates select="edm:realizes"/>
			<xsl:apply-templates select="edm:type"/>
			<xsl:apply-templates select="owl:sameAs"/>
		</xsl:element>
	</xsl:template>


	<xsl:template match="ore:Aggregation">
		<xsl:element name="ore:Aggregation">
			<xsl:copy-of select="@*"/>
			<xsl:apply-templates select="edm:aggregatedCHO"/>
			<xsl:apply-templates select="edm:dataProvider"/>
			<xsl:apply-templates select="edm:hasView"/>
			<xsl:apply-templates select="edm:isShownAt"/>
			<xsl:apply-templates select="edm:isShownBy"/>
			<xsl:apply-templates select="edm:object"/>
			<xsl:apply-templates select="edm:provider"/>
			<xsl:apply-templates select="dc:rights"/>
			<xsl:apply-templates select="edm:rights"/>
			<xsl:apply-templates select="edm:ugc"/>
			<xsl:apply-templates select="edm:intermediateProvider"/>
			</xsl:element>
	</xsl:template>

	<xsl:template match="edm:EuropeanaAggregation">
		<xsl:element name="edm:EuropeanaAggregation">
			<xsl:copy-of select="@*"/>
			<xsl:apply-templates select="dc:creator"/>
			<xsl:apply-templates select="edm:aggregatedCHO"/>
			<xsl:apply-templates select="edm:dataProvider"/>
			<xsl:apply-templates select="edm:collectionName"/>
			<xsl:apply-templates select="edm:datasetName"/>
			<xsl:apply-templates select="edm:country"/>
			<xsl:apply-templates select="edm:hasView"/>
			<xsl:apply-templates select="edm:isShownBy"/>
			<xsl:apply-templates select="edm:preview"/>
			<xsl:apply-templates select="edm:landingPage"/>
			<xsl:apply-templates select="edm:language"/>
			<xsl:apply-templates select="edm:provider"/>
			<xsl:apply-templates select="edm:rights"/>
			<xsl:apply-templates select="ore:aggregates"/>
		</xsl:element>
	</xsl:template>

	<xsl:template match="ore:Proxy">
		<xsl:element name="ore:Proxy">
			<xsl:copy-of select="@*"/>
			<xsl:apply-templates select="dc:contributor"/>
			<xsl:apply-templates select="dc:coverage"/>
			<xsl:apply-templates select="dc:creator"/>
			<xsl:apply-templates select="dc:date"/>
			<xsl:apply-templates select="dc:description"/>
			<xsl:apply-templates select="dc:format"/>
			<xsl:apply-templates select="dc:identifier"/>
			<xsl:apply-templates select="dc:language"/>
			<xsl:apply-templates select="dc:publisher"/>
			<xsl:apply-templates select="dc:relation"/>
			<xsl:apply-templates select="dc:rights"/>
			<xsl:apply-templates select="dc:source"/>
			<xsl:apply-templates select="dc:subject"/>
			<xsl:apply-templates select="dc:title"/>
			<xsl:apply-templates select="dc:type"/>
			<xsl:apply-templates select="dcterms:alternative"/>
			<xsl:apply-templates select="dcterms:conformsTo"/>
			<xsl:apply-templates select="dcterms:created"/>
			<xsl:apply-templates select="dcterms:extent"/>
			<xsl:apply-templates select="dcterms:hasFormat"/>
			<xsl:apply-templates select="dcterms:hasPart"/>
			<xsl:apply-templates select="dcterms:hasVersion"/>
			<xsl:apply-templates select="dcterms:isFormatOf"/>
			<xsl:apply-templates select="dcterms:isPartOf"/>
			<xsl:apply-templates select="dcterms:isReferencedBy"/>
			<xsl:apply-templates select="dcterms:isReplacedBy"/>
			<xsl:apply-templates select="dcterms:isRequiredBy"/>
			<xsl:apply-templates select="dcterms:issued"/>
			<xsl:apply-templates select="dcterms:isVersionOf"/>
			<xsl:apply-templates select="dcterms:medium"/>
			<xsl:apply-templates select="dcterms:provenance"/>
			<xsl:apply-templates select="dcterms:references"/>
			<xsl:apply-templates select="dcterms:replaces"/>
			<xsl:apply-templates select="dcterms:requires"/>
			<xsl:apply-templates select="dcterms:spatial"/>
			<xsl:apply-templates select="dcterms:tableOfContents"/>
			<xsl:apply-templates select="dcterms:temporal"/>
			<xsl:apply-templates select="edm:currentLocation"/>
			<xsl:apply-templates select="edm:hasMet"/>
			<xsl:apply-templates select="edm:hasType"/>
			<xsl:apply-templates select="edm:incorporates"/>
			<xsl:apply-templates select="edm:isDerivativeOf"/>
			<xsl:apply-templates select="edm:isNextInSequence"/>
			<xsl:apply-templates select="edm:isRelatedTo"/>
			<xsl:apply-templates select="edm:isRepresentationOf"/>
			<xsl:apply-templates select="edm:isSimilarTo"/>
			<xsl:apply-templates select="edm:isSuccessorOf"/>
			<xsl:apply-templates select="edm:realizes"/>
			<xsl:apply-templates select="edm:europeanaProxy"/>
			<xsl:apply-templates select="edm:userTag"/>
			<xsl:apply-templates select="edm:year"/>
			<xsl:apply-templates select="ore:proxyFor"/>
			<xsl:apply-templates select="ore:proxyIn"/>
			<xsl:apply-templates select="ore:lineage"/>
			<xsl:apply-templates select="edm:type"/>
			<xsl:apply-templates select="owl:sameAs"/>
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
			<xsl:copy-of select="@*"/>
			<xsl:apply-templates select="node()"/>
		</xsl:element>
	</xsl:template>

</xsl:stylesheet>
