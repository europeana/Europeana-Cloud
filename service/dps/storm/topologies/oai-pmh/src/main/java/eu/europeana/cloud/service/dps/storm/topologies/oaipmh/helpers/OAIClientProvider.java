package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers;

import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.client.OAIClient;

import java.io.Serializable;

/**
 * Provides {@link OAIClient} to one who is interested to.
 *
 */
public class OAIClientProvider implements Serializable {

    public OAIClient provide(String baseUrl){
        return new HttpOAIClient(baseUrl);
    }
}
