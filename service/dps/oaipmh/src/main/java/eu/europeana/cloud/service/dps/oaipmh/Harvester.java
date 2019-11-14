package eu.europeana.cloud.service.dps.oaipmh;

import com.google.common.base.Throwables;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.model.oaipmh.MetadataFormat;
import org.dspace.xoai.model.oaipmh.Verb;
import org.dspace.xoai.serviceprovider.exceptions.BadArgumentException;
import org.dspace.xoai.serviceprovider.exceptions.HttpException;
import org.dspace.xoai.serviceprovider.parameters.GetRecordParameters;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.dspace.xoai.serviceprovider.parameters.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPathExpression;
import java.io.IOException;
import java.io.InputStream;

/**
 * Class harvest record from the external OAI-PMH repository.
 */
public class Harvester {

    private static final Logger LOGGER = LoggerFactory.getLogger(Harvester.class);
    private final int numberOfRetries;
    private final int timeBetweenRetries;

    private static final ConnectionFactory DEFAULT_CONNECTION_FACTORY = new ConnectionFactory() {
        @Override
        public CustomConnection createConnection(String oaiPmhEndpoint) {
            return new CustomConnection(oaiPmhEndpoint);
        }
    };

    public Harvester(int numberOfRetries, int timeBetweenRetries) {
        this.numberOfRetries = numberOfRetries;
        this.timeBetweenRetries = timeBetweenRetries;
    }

    /**
     * Harvest record
     *
     * @param oaiPmhEndpoint OAI-PMH endpoint
     * @param recordId       record id
     * @param metadataPrefix metadata prefix
     * @param expression     XPATH expression
     * @return return metadata
     * @throws HarvesterException
     */
    public InputStream harvestRecord(String oaiPmhEndpoint, String recordId, String metadataPrefix,
            XPathExpression expression, XPathExpression statusCheckerExpression)
            throws HarvesterException {
        return harvestRecord(DEFAULT_CONNECTION_FACTORY, oaiPmhEndpoint, recordId, metadataPrefix,
                expression, statusCheckerExpression);
    }

    InputStream harvestRecord(ConnectionFactory connectionFactory, String oaiPmhEndpoint,
            String recordId, String metadataPrefix, XPathExpression expression,
            XPathExpression statusCheckerExpression) throws HarvesterException {

        GetRecordParameters params = new GetRecordParameters().withIdentifier(recordId).withMetadataFormatPrefix(metadataPrefix);
        int retries = numberOfRetries;
        while (true) {
            final CustomConnection client = connectionFactory.createConnection(oaiPmhEndpoint);
            try {
                String record = client.execute(Parameters.parameters().withVerb(Verb.Type.GetRecord).include(params));
                XmlXPath xmlXPath = new XmlXPath(record);
                if (xmlXPath.isDeletedRecord(statusCheckerExpression)) {
                    retries = 0;
                    throw new HarvesterException("The record is deleted");
                }
                return xmlXPath.xpath(expression);
            } catch (IOException | RuntimeException | HttpException e) {
                if (retries > 0) {
                    retries--;
                    LOGGER.warn("Error harvesting record {}. Retries left:{} ", recordId, retries);
                    try {
                        Thread.sleep(timeBetweenRetries);
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

    public interface ConnectionFactory {

        CustomConnection createConnection(String oaiPmhEndpoint);
    }

    public Set<String> getSchemas(String fileUrl, Set<String> excludedSchemas) {
        OAIHelper oaiHelper = createOaiHelper(fileUrl);
        Iterator<MetadataFormat> metadataFormatIterator = oaiHelper.listSchemas();
        Set<String> schemas = new HashSet<>();
        while (metadataFormatIterator.hasNext()) {
            String schema = metadataFormatIterator.next().getMetadataPrefix();
            if (excludedSchemas == null || !excludedSchemas.contains(schema)) {
                schemas.add(schema);
            }
        }
        return schemas;
    }

    OAIHelper createOaiHelper(String fileUrl) {
        return new OAIHelper(fileUrl, numberOfRetries, timeBetweenRetries);
    }

    public List<String> harvestIdentifiers(String schema, String dataset, Date fromDate,
            Date untilDate, String fileUrl, Set<String> excludedSets, CancelTrigger cancelTrigger)
            throws HarvesterException {
        final SourceProvider sourceProvider = new SourceProvider();
        final ListIdentifiersParameters parameters = configureParameters(schema, dataset, fromDate,
                untilDate);
        try {
            return parseHeaders(sourceProvider.provide(fileUrl).listIdentifiers(parameters),
                    excludedSets, cancelTrigger);
        } catch (BadArgumentException e) {
            throw new HarvesterException("Unable to harvest identifiers.", e);
        }
    }

    /**
     * Configure request parameters
     *
     * @return object representing parameters for ListIdentifiers request
     */
    private ListIdentifiersParameters configureParameters(String schema, String dataset,
            Date fromDate, Date untilDate) {
        ListIdentifiersParameters parameters = ListIdentifiersParameters.request()
                .withMetadataPrefix(schema);
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
     * @param excludedSets   sets to exclude
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
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting the next batch {}", e.getMessage());
                    throw new IllegalStateException(" Error while getting the next batch of identifiers from the oai end-point.", e);
                }
            }
        }
    }

    private void waitForSpecificTime() {
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
     * @param header       header to filter
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

    public interface CancelTrigger {

        boolean shouldCancel();
    }
}


