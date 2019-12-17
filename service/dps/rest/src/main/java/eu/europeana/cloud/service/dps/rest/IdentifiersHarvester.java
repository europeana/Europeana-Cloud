package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.dps.Harvest;
import eu.europeana.cloud.service.dps.OAIHeader;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.serviceprovider.ServiceProvider;
import org.dspace.xoai.serviceprovider.client.HttpOAIClient;
import org.dspace.xoai.serviceprovider.exceptions.BadArgumentException;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class IdentifiersHarvester {

    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentifiersHarvester.class);

    private ListIdentifiersParameters listIdentifiersParameters;
    private ServiceProvider provider;

    public IdentifiersHarvester(Harvest harvest) {
        listIdentifiersParameters = prepareParameters(harvest);
        HttpOAIClient c = new HttpOAIClient(harvest.getUrl());
        provider = new ServiceProvider(new org.dspace.xoai.serviceprovider.model.Context().withOAIClient(new HttpOAIClient(harvest.getUrl())));
    }

    public IdentifiersIterator harvestIdentifiers() throws BadArgumentException {
        Iterator<Header> iterator = provider.listIdentifiers(listIdentifiersParameters);
        return new IdentifiersIterator(iterator);
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
        if (harvest.getSetSpec() != null) {
            parameters.withSetSpec(harvest.getSetSpec());
        }
        return parameters;
    }

    public static class IdentifiersIterator implements Iterator<OAIHeader> {

        Iterator<Header> headerIterator;

        public IdentifiersIterator(Iterator<Header> headerIterator) {
            this.headerIterator = headerIterator;
        }

        @Override
        public boolean hasNext() {
            int retries = DEFAULT_RETRIES;
            while (true) {
                try {
                    return headerIterator.hasNext();
                } catch (Exception e) {
                    if (retries-- > 0) {
                        LOGGER.warn("Error while getting the next batch: {}. Retries left {}. The cause of the error is {}", e.getMessage(), retries, e.getMessage() + " " + e.getCause());
                        waitForSpecificTime();
                    } else {
                        LOGGER.error("Error while getting the next batch {}", e.getMessage());
                        throw new IllegalStateException(" Error while getting the next batch of identifiers from the oai end-point.", e);
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

        protected void waitForSpecificTime() {
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
                LOGGER.error(e1.getMessage());
            }
        }
    }
}
