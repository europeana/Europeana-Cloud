package eu.europeana.cloud.service.dps.rest.oaiharvest;

import org.dspace.xoai.model.oaipmh.Granularity;
import org.dspace.xoai.model.oaipmh.MetadataFormat;
import org.dspace.xoai.serviceprovider.ServiceProvider;
import org.dspace.xoai.serviceprovider.client.HttpOAIClient;
import org.dspace.xoai.serviceprovider.client.OAIClient;
import org.dspace.xoai.serviceprovider.exceptions.IdDoesNotExistException;
import org.dspace.xoai.serviceprovider.exceptions.InvalidOAIResponse;
import org.dspace.xoai.serviceprovider.model.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;

/**
 * Created by Tarek on 7/5/2017.
 */
public class OAIHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(OAIHelper.class);

    // default number of retries
    public static final int DEFAULT_RETRIES = 3;

    public static final int SLEEP_TIME = 5000;


    private String resourceURL;

    public OAIHelper(String resourceURL) {
        this.resourceURL = resourceURL;
    }

    private ServiceProvider initServiceProvider() {
        OAIClient client = new HttpOAIClient(resourceURL);
        return new ServiceProvider(new Context().withOAIClient(client));
    }

    public Iterator<MetadataFormat> listSchemas() {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                ServiceProvider serviceProvider = initServiceProvider();
                return serviceProvider.listMetadataFormats();
            } catch (InvalidOAIResponse e) {
                retries = handleExceptionWithRetries(retries, e, "Retrieving metadata schemas");
            } catch (IdDoesNotExistException e) {
                //will never happen in here as we don't specify "identifier" argument
            }
        }
    }

    public Date getEarlierDate() {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                ServiceProvider serviceProvider = initServiceProvider();
                return serviceProvider.identify().getEarliestDatestamp();
            } catch (InvalidOAIResponse e) {
                retries = handleExceptionWithRetries(retries, e, "Retrieving the earliest timestamp");
            }
        }
    }

    public Granularity getGranularity() {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                ServiceProvider serviceProvider = initServiceProvider();
                return serviceProvider.identify().getGranularity();
            } catch (InvalidOAIResponse e) {
                retries = handleExceptionWithRetries(retries, e, "Retrieving the granularity");
            }
        }
    }

    private int handleExceptionWithRetries(int retries, InvalidOAIResponse e, String message){
        if (retries-- > 0) {
            LOGGER.warn("Error {} . Retries left: {}", message, retries);
            try {
                Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
                LOGGER.error(e1.getMessage());
            }
        } else {
            LOGGER.error("{} failed.", message);
            throw e;
        }
        return retries;
    }
}
