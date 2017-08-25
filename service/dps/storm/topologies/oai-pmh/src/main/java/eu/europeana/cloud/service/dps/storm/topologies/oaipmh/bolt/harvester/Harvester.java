package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester;

import com.lyncode.xoai.model.oaipmh.Verb;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.OAIRequestException;
import com.lyncode.xoai.serviceprovider.parameters.GetRecordParameters;
import com.lyncode.xoai.serviceprovider.parameters.Parameters;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;

import java.io.InputStream;
import java.io.Serializable;

/**
 * Class harvest record from the external OAI-PMH repository.
 */
public class Harvester implements Serializable {
    private static final String METADATA_XPATH = "/*[local-name()='OAI-PMH']" +
            "/*[local-name()='GetRecord']" +
            "/*[local-name()='record']"+
            "/*[local-name()='metadata']" +
            "/child::*";

    /**
     * Harvest record
     * @param oaiPmhEndpoint OAI-PMH endpoint
     * @param recordId record id
     * @param metadataPrefix metadata prefix
     * @return return metadata
     * @throws HarvesterException
     */
    public InputStream harvestRecord(String oaiPmhEndpoint, String recordId, String metadataPrefix)
            throws HarvesterException{


        GetRecordParameters params = new GetRecordParameters().withIdentifier(recordId).withMetadataFormatPrefix(metadataPrefix);
        OAIClient client = new HttpOAIClient(oaiPmhEndpoint);
        try {
            final InputStream execute = client.execute(Parameters.parameters().withVerb(Verb.Type.GetRecord).include(params));
            return new XmlXPath(execute).xpath(METADATA_XPATH);
        } catch (OAIRequestException e) {
            throw new HarvesterException(String.format("Problem with harvesting record %1$s for endpoint %2$s",
                    recordId, oaiPmhEndpoint), e);
        }
    }
}


