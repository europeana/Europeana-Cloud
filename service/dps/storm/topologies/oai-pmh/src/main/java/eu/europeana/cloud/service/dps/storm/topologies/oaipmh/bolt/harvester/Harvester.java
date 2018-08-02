package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester;

import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.exceptions.HarvesterException;
import org.apache.commons.io.IOUtils;
import org.dspace.xoai.model.oaipmh.Verb;
import org.dspace.xoai.serviceprovider.client.HttpOAIClient;
import org.dspace.xoai.serviceprovider.client.OAIClient;
import org.dspace.xoai.serviceprovider.parameters.GetRecordParameters;
import org.dspace.xoai.serviceprovider.parameters.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPathExpression;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * Class harvest record from the external OAI-PMH repository.
 */
public class Harvester implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Harvester.class);


    /**
     * Harvest record
     *
     * @param oaiPmhEndpoint OAI-PMH endpoint
     * @param recordId       record id
     * @param metadataPrefix metadata prefix
     *  @param expression XPATH expression

     * @return return metadata
     * @throws HarvesterException
     * @throws IOException
     */
    public InputStream harvestRecord(String oaiPmhEndpoint, String recordId, String metadataPrefix, XPathExpression expression)
            throws HarvesterException, IOException {


        GetRecordParameters params = new GetRecordParameters().withIdentifier(recordId).withMetadataFormatPrefix(metadataPrefix);
        int retries = AbstractDpsBolt.DEFAULT_RETRIES;
        while (true) {
            OAIClient client = new MyOAIHarvester(oaiPmhEndpoint);
            try (final InputStream record = client.execute(Parameters.parameters().withVerb(Verb.Type.GetRecord).include(params))) {
                return new XmlXPath(record).xpath(expression);
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error harvesting record {}. Retries left:{} ", recordId, retries);
                    try {
                        Thread.sleep(AbstractDpsBolt.SLEEP_TIME);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        LOGGER.error(Throwables.getStackTraceAsString(ex));
                    }
                } else {
                    throw new HarvesterException(String.format("Problem with harvesting record %1$s for endpoint %2$s because of: %3$s",
                            recordId, oaiPmhEndpoint, e.getMessage()), e);
                }
            }
        }
    }
}


