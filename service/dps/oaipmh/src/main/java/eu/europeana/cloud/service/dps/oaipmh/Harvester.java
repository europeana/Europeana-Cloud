package eu.europeana.cloud.service.dps.oaipmh;

import java.io.InputStream;
import java.util.Date;
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
     * Return the schemas.
     *
     * @param oaiPmhEndpoint Base URL of the OAI-PMH endpoint
     * @param excludedSchemas The schemas to exclude.
     * @return The schemas that are supported (and that are not excluded).
     * @throws HarvesterException In case there was some problem performing this operation.
     */
    Set<String> getSchemas(String oaiPmhEndpoint, Set<String> excludedSchemas)
            throws HarvesterException;

    /**
     * Get the identifiers for a given OAI-PMH endpoint.
     *
     * @param metadataPrefix The metadata prefix (schema)
     * @param dataset The dataset (set spec)
     * @param fromDate The from date
     * @param untilDate The until date
     * @param oaiPmhEndpoint Base URL of the OAI-PMH endpoint
     * @param excludedSets The sets (set spec) to exclude
     * @param cancelTrigger The cancel trigger
     * @return The identifiers.
     * @throws HarvesterException In case there was some problem performing this operation.
     */
    List<String> harvestIdentifiers(String metadataPrefix, String dataset, Date fromDate,
            Date untilDate, String oaiPmhEndpoint, Set<String> excludedSets,
            CancelTrigger cancelTrigger)
            throws HarvesterException;

    /**
     * This interface can be used to repeatedly check whether a job should be cancelled due to
     * external circumstances.
     */
    interface CancelTrigger {

        /**
         * @return Whether the job currently in progress should be cancelled due to external
         * circumstances.
         */
        boolean shouldCancel();
    }
}
