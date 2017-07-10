package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common;

import com.lyncode.xoai.model.oaipmh.MetadataFormat;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.model.Context;

import java.util.Date;
import java.util.Iterator;

/**
 * Created by Tarek on 7/5/2017.
 */
public class OAIHelper {
    private ServiceProvider serviceProvider;

    public OAIHelper(String resourceURL) {
        OAIClient client = new HttpOAIClient(resourceURL);
        serviceProvider = new ServiceProvider(new Context().withOAIClient(client));
    }

    public Iterator<MetadataFormat> listSchemas() {
        return serviceProvider.listMetadataFormats();
    }

    public Date getEarlierDate() {
        return serviceProvider.identify().getEarliestDatestamp();
    }
}
