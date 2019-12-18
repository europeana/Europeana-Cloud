package eu.europeana.cloud.service.dps.oaipmh;

import eu.europeana.cloud.service.dps.Harvest;
import eu.europeana.cloud.service.dps.OAIHeader;

import java.io.InputStream;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.xml.xpath.XPathExpression;

/**
 * Classes that implement this interface provide functionality regarding OAI-PMH harvesting.
 */
public interface Harvester {

    /**
     * Harvest record.
     *
     * @param oaiPmhEndpoint Base URL of the OAI-PMH endpoint
     * @param recordId record id
     * @param metadataPrefix metadata prefix (schema)
     * @param expression XPATH expression for obtaining the data.
     * @param statusCheckerExpression XPATH expression for checking the status.
     * @return The record.
     * @throws HarvesterException In case there was some problem performing this operation.
     */
    InputStream harvestRecord(String oaiPmhEndpoint, String recordId, String metadataPrefix,
            XPathExpression expression, XPathExpression statusCheckerExpression)
            throws HarvesterException;

    /**
     * Harvest identifiers from given location describet in harvest parameter
     * @param harvest Descriptor with harvesting oinfo like url, date from date to, metadata prefix etc
     * @return Iterator to iterate over harvested data
     * @throws HarvesterException In case there was some problem performing this operation.
     */
    Iterator<OAIHeader> harvestIdentifiers(Harvest harvest) throws HarvesterException;

}
