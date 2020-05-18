package eu.europeana.cloud.service.dps.oaipmh;

import eu.europeana.cloud.service.dps.Harvest;
import eu.europeana.cloud.service.dps.OAIHeader;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.model.oaipmh.Verb;
import org.dspace.xoai.serviceprovider.ServiceProvider;
import org.dspace.xoai.serviceprovider.exceptions.BadArgumentException;
import org.dspace.xoai.serviceprovider.exceptions.HttpException;
import org.dspace.xoai.serviceprovider.parameters.GetRecordParameters;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.dspace.xoai.serviceprovider.parameters.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPathExpression;
import java.io.InputStream;
import java.util.Iterator;

public class HarvesterImpl implements Harvester {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvesterImpl.class);

    private static final ConnectionFactory DEFAULT_CONNECTION_FACTORY = new HarvesterImpl.ConnectionFactory() {
        @Override
        public OaiPmhConnection createConnection(String oaiPmhEndpoint, Parameters parameters) {
            return new OaiPmhConnection(oaiPmhEndpoint, parameters);
        }
    };

    private final int numberOfRetries;
    private final int timeBetweenRetries;

    /**
     * Constructor.
     *
     * @param numberOfRetries The number of times we retry a connection.
     * @param timeBetweenRetries The time we leave between two successive retries.
     */
    HarvesterImpl(int numberOfRetries, int timeBetweenRetries) {
        this.numberOfRetries = numberOfRetries;
        this.timeBetweenRetries = timeBetweenRetries;
    }


    @Override
    public InputStream harvestRecord(String oaiPmhEndpoint, String recordId, String metadataPrefix,
                                     XPathExpression expression, XPathExpression statusCheckerExpression)
            throws HarvesterException {
        return harvestRecord(DEFAULT_CONNECTION_FACTORY, oaiPmhEndpoint, recordId, metadataPrefix,
                expression, statusCheckerExpression);
    }

    InputStream harvestRecord(ConnectionFactory connectionFactory, String oaiPmhEndpoint,
                              String recordId, String metadataPrefix, XPathExpression expression,
                              XPathExpression statusCheckerExpression) throws HarvesterException {

        final GetRecordParameters params = new GetRecordParameters().withIdentifier(recordId).withMetadataFormatPrefix(metadataPrefix);
        final Parameters parameters = Parameters.parameters().withVerb(Verb.Type.GetRecord).include(params);
        final OaiPmhConnection connection = connectionFactory.createConnection(oaiPmhEndpoint, parameters);

        int retries = numberOfRetries;
        while (true) {
            try {
                String record = connection.execute();
                XmlXPath xmlXPath = new XmlXPath(record);
                if ("deleted".equalsIgnoreCase(xmlXPath.xpathToString(statusCheckerExpression))) {
                    retries = 0;
                    throw new HarvesterException("The record is deleted");
                }
                return xmlXPath.xpathToStream(expression);
            } catch (RuntimeException | HttpException e) {
                if (retries > 0) {
                    retries--;
                    LOGGER.warn("Error harvesting record {}. Retries left:{} ", recordId, retries);
                    waitForSpecificTime();
                } else {
                    throw new HarvesterException(String.format("Problem with harvesting record %1$s for endpoint %2$s because of: %3$s",
                            recordId, oaiPmhEndpoint, e.getMessage()), e);
                }
            }
        }
    }

    @Override
    public Iterator<OAIHeader> harvestIdentifiers(Harvest harvest) throws HarvesterException {
        try {
            ListIdentifiersParameters listIdentifiersParameters = prepareParameters(harvest);
            HarvesterHttpOAIClient httpOAIClient = new HarvesterHttpOAIClient(harvest.getUrl());
            ServiceProvider provider = new ServiceProvider(new org.dspace.xoai.serviceprovider.model.Context().withOAIClient(httpOAIClient));

            Iterator<Header> iterator = provider.listIdentifiers(listIdentifiersParameters);
            return new OAIHeaderIterator(iterator, numberOfRetries, httpOAIClient);
        } catch(BadArgumentException bae) {
            throw new HarvesterException(bae.getMessage(), bae);
        }
    }

    private ListIdentifiersParameters prepareParameters(Harvest harvest) {
        ListIdentifiersParameters parameters = ListIdentifiersParameters.request()
                .withMetadataPrefix(harvest.getMetadataPrefix());
        if (harvest.getFrom() != null) {
            parameters.withFrom(harvest.getFrom());
        }
        if (harvest.getUntil() != null) {
            parameters.withUntil(harvest.getUntil());
        }
        if (harvest.getOaiSetSpec() != null) {
            parameters.withSetSpec(harvest.getOaiSetSpec());
        }
        return parameters;
    }

    protected void waitForSpecificTime() {
        try {
            Thread.sleep(timeBetweenRetries);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.error(ie.getMessage());
        }
    }

    /**
     * Auxiliaru interface for harvesting records
     */
    interface ConnectionFactory {
        /**
         * Construct a connection based on the supplied information.
         *
         * @param oaiPmhEndpoint The base URL.
         * @param parameters The parameters.
         * @return A connection instance.
         */
        OaiPmhConnection createConnection(String oaiPmhEndpoint, Parameters parameters);
    }

    /**
     * Iterator for harvesting
     */
    public class OAIHeaderIterator implements Iterator<OAIHeader> {

        private Iterator<Header> headerIterator;
        private final int numberOfRetries;
        private HarvesterHttpOAIClient oaiClient;

        public OAIHeaderIterator(Iterator<Header> headerIterator, int numberOfRetries, HarvesterHttpOAIClient oaiClient) {
            this.headerIterator = headerIterator;
            this.numberOfRetries = numberOfRetries;
            this.oaiClient = oaiClient;
        }

        @Override
        public boolean hasNext() {
            int retries = numberOfRetries;
            while (true) {
                try {
                    return headerIterator.hasNext();
                } catch (Exception e) {
                    if (retries-- > 0) {
                        if(oaiClient != null) {
                            oaiClient.closeResponse();
                        }

                        LOGGER.warn("Error while getting the next batch: {}. Retries left {}. The cause of the error is {}", e.getMessage(), retries, e.getMessage() + " " + e.getCause());
                        HarvesterImpl.this.waitForSpecificTime();
                    } else {
                        LOGGER.error("Error while getting the next batch {}", e.getMessage());
                        throw new IllegalStateException(
                                String.format("Error while getting the next batch of identifiers from the oai end-point." +
                                        " Number of attempts: %d. Time between attempts: %.0g seconds",
                                        numberOfRetries, (double)timeBetweenRetries/1000.0 ), e);
                    }
                }
            }
        }

        @Override
        public OAIHeader next() {
            Header header = headerIterator.next();
            return OAIHeader.builder()
                    .identifier(header.getIdentifier())
                    .build();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("OAI-PMH is a read-only interface");
        }
    }
}
