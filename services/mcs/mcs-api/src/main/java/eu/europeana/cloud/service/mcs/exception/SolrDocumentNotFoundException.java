package eu.europeana.cloud.service.mcs.exception;

public class SolrDocumentNotFoundException
	extends Exception
{

	public SolrDocumentNotFoundException(String versionId)
	{
		super(String.format("Solr document not found for %s", versionId));
	}
}
