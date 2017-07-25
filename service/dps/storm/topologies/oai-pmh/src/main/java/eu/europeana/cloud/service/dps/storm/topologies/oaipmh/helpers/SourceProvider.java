package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers;

import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.model.Context;

import java.io.Serializable;

public class SourceProvider implements Serializable {
    public ServiceProvider provide(String baseUrl) {
        return new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(baseUrl)));
    }
}
