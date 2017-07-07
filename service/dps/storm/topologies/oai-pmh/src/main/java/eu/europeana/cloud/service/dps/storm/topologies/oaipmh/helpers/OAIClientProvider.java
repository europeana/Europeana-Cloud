package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers;

import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.client.OAIClient;

/**
 * Created by pwozniak on 7/4/17.
 */
public class OAIClientProvider {

    public OAIClient provide(String baseUrl){
        return new HttpOAIClient(baseUrl);
    }
}
