package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common;

import com.lyncode.xoai.model.oaipmh.Granularity;
import com.lyncode.xoai.model.oaipmh.MetadataFormat;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.InvalidOAIResponse;
import com.lyncode.xoai.serviceprovider.model.Context;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Tarek on 7/5/2017.
 */
public class OAIHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(OAIHelper.class);

    private String resourceURL;

    public OAIHelper(String resourceURL) {
        this.resourceURL = resourceURL;
    }

    private ServiceProvider initServiceProvider() {
        OAIClient client = new HttpOAIClient(resourceURL);
        return new ServiceProvider(new Context().withOAIClient(client));
    }

    public Iterator<MetadataFormat> listSchemas() {
        int retries = AbstractDpsBolt.DEFAULT_RETRIES;

        while (true) {
            try {
                ServiceProvider serviceProvider = initServiceProvider();
                return serviceProvider.listMetadataFormats();
            }
            catch (InvalidOAIResponse e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error retrieving metadata schemas. Retries left: " + retries);
                    try {
                        Thread.sleep(AbstractDpsBolt.SLEEP_TIME);
                    } catch (InterruptedException ex) {
                        LOGGER.error(ex.getMessage());
                    }
                }
                else {
                    LOGGER.error("Retrieving metadata schemas failed.");
                    throw e;
                }
            }
        }
    }

    public Date getEarlierDate() {
        int retries = AbstractDpsBolt.DEFAULT_RETRIES;

        while (true) {
            try {
                ServiceProvider serviceProvider = initServiceProvider();
                return serviceProvider.identify().getEarliestDatestamp();
            }
            catch (InvalidOAIResponse e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error retrieving the earliest timestamp. Retries left: " + retries);
                    try {
                        Thread.sleep(AbstractDpsBolt.SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        LOGGER.error(e1.getMessage());
                    }
                }
                else {
                    LOGGER.error("Retrieving the earliest timestamp failed.");
                    throw e;
                }
            }
        }
    }

    public Granularity getGranularity(){
        int retries = AbstractDpsBolt.DEFAULT_RETRIES;

        while (true) {
            try {
                ServiceProvider serviceProvider = initServiceProvider();
                return serviceProvider.identify().getGranularity();
            }
            catch (InvalidOAIResponse e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error retrieving the granularity. Retries left: " + retries);
                    try {
                        Thread.sleep(AbstractDpsBolt.SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        LOGGER.error(e1.getMessage());
                    }
                }
                else {
                    LOGGER.error("Retrieving granularity failed.");
                    throw e;
                }
            }
        }
    }
}
