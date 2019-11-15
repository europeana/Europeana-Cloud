package eu.europeana.cloud.service.dps.oaipmh;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.model.oaipmh.MetadataFormat;
import org.dspace.xoai.model.oaipmh.Verb;
import org.dspace.xoai.serviceprovider.ServiceProvider;
import org.dspace.xoai.serviceprovider.client.HttpOAIClient;
import org.dspace.xoai.serviceprovider.exceptions.BadArgumentException;
import org.dspace.xoai.serviceprovider.exceptions.HttpException;
import org.dspace.xoai.serviceprovider.exceptions.IdDoesNotExistException;
import org.dspace.xoai.serviceprovider.exceptions.InvalidOAIResponse;
import org.dspace.xoai.serviceprovider.model.Context;
import org.dspace.xoai.serviceprovider.parameters.GetRecordParameters;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.dspace.xoai.serviceprovider.parameters.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPathExpression;
import java.io.InputStream;

/**
 * This class implements the contract to provide harvesting functionality.
 */
class HarvesterImpl implements Harvester {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvesterImpl.class);

    private final int numberOfRetries;
    private final int timeBetweenRetries;

    private static final ConnectionFactory DEFAULT_CONNECTION_FACTORY = new ConnectionFactory() {
        @Override
        public OaiPmhConnection createConnection(String oaiPmhEndpoint, Parameters parameters) {
            return new OaiPmhConnection(oaiPmhEndpoint, parameters);
        }
    };

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
                    waitForNextRetry();
                } else {
                    throw new HarvesterException(String.format("Problem with harvesting record %1$s for endpoint %2$s because of: %3$s",
                            recordId, oaiPmhEndpoint, e.getMessage()), e);
                }
            }
        }
    }

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

    @Override
    public Set<String> getSchemas(String oaiPmhEndpoint, Set<String> excludedSchemas)
            throws HarvesterException {
        Iterator<MetadataFormat> metadataFormatIterator = getSchemaIterator(oaiPmhEndpoint);
        Set<String> schemas = new HashSet<>();
        while (metadataFormatIterator.hasNext()) {
            String schema = metadataFormatIterator.next().getMetadataPrefix();
            if (excludedSchemas == null || !excludedSchemas.contains(schema)) {
                schemas.add(schema);
            }
        }
        return schemas;
    }

    Iterator<MetadataFormat> getSchemaIterator(String oaiPmhEndpoint) throws HarvesterException {
        int retries = numberOfRetries;
        while (true) {
            try {
                ServiceProvider serviceProvider = createServiceProvider(oaiPmhEndpoint);
                return serviceProvider.listMetadataFormats();
            } catch (InvalidOAIResponse e) {
                if (retries > 0) {
                    retries--;
                    LOGGER.warn("Error while retrieving metadata schemas. Retries left: {}", retries);
                    waitForNextRetry();
                } else {
                    LOGGER.error("Error while retrieving metadata schemas.");
                    throw new HarvesterException("Problem while retrieving metadata schemas.", e);
                }
            } catch (IdDoesNotExistException e) {
                //will never happen in here as we don't specify "identifier" argument
                throw new HarvesterException("Unexpected exception.", e);
            }
        }
    }

    @Override
    public List<String> harvestIdentifiers(String metadataPrefix, String dataset, Date fromDate,
            Date untilDate, String oaiPmhEndpoint, Set<String> excludedSets, CancelTrigger cancelTrigger)
            throws HarvesterException {
        final ListIdentifiersParameters parameters = configureParameters(metadataPrefix, dataset, fromDate,
                untilDate);
        try {
            return parseHeaders(createServiceProvider(oaiPmhEndpoint).listIdentifiers(parameters),
                    excludedSets, cancelTrigger);
        } catch (BadArgumentException e) {
            throw new HarvesterException("Unable to harvest identifiers.", e);
        }
    }

    static ServiceProvider createServiceProvider(String oaiPmhEndpoint) {
        return new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(oaiPmhEndpoint)));
    }

    /**
     * Create ListIdentifiers request parameters given the input.
     *
     * @return object representing parameters for ListIdentifiers request
     */
    private ListIdentifiersParameters configureParameters(String metadataPrefix, String dataset,
            Date fromDate, Date untilDate) {
        ListIdentifiersParameters parameters = ListIdentifiersParameters.request()
                .withMetadataPrefix(metadataPrefix);
        if (fromDate != null) {
            parameters.withFrom(fromDate);
        }
        if (untilDate != null) {
            parameters.withUntil(untilDate);
        }
        if (dataset != null) {
            parameters.withSetSpec(dataset);
        }
        return parameters;
    }

    /**
     * Parse headers returned by the OAI-PMH source
     *
     * @param headerIterator iterator of headers returned by the source
     * @param excludedSets sets to exclude
     * @param cancelTrigger The cancel trigger
     * @return number of harvested identifiers
     */
    private List<String> parseHeaders(Iterator<Header> headerIterator, Set<String> excludedSets,
            CancelTrigger cancelTrigger) {
        if (headerIterator == null) {
            throw new IllegalArgumentException("Header iterator is null");
        }
        final List<String> result = new ArrayList<>();
        while (hasNext(headerIterator) && !cancelTrigger.shouldCancel()) {
            Header header = headerIterator.next();
            if (filterHeader(header, excludedSets)) {
                result .add(header.getIdentifier());
            }
        }
        return result;
    }

    private boolean hasNext(Iterator<Header> headerIterator) {
        int retries = numberOfRetries;
        while (true) {
            try {
                return headerIterator.hasNext();
            } catch (RuntimeException e) {
                if (retries > 0) {
                    retries--;
                    LOGGER.warn(
                            "Error while getting the next batch: {}. Retries left {}. The cause of the error is {}",
                            e.getMessage(), retries, e.getMessage() + " " + e.getCause());
                    waitForNextRetry();
                } else {
                    LOGGER.error("Error while getting the next batch {}", e.getMessage());
                    throw new IllegalStateException(" Error while getting the next batch of identifiers from the oai end-point.", e);
                }
            }
        }
    }

    private void waitForNextRetry() {
        try {
            Thread.sleep(timeBetweenRetries);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }

    /**
     * Filter header by checking whether it belongs to any of excluded sets.
     *
     * @param header header to filter
     * @param excludedSets sets to exclude
     */
    private boolean filterHeader(Header header, Set<String> excludedSets) {
        if (excludedSets != null && !excludedSets.isEmpty()) {
            for (String set : excludedSets) {
                if (header.getSetSpecs().contains(set)) {
                    return false;
                }
            }
        }
        return true;
    }
}


