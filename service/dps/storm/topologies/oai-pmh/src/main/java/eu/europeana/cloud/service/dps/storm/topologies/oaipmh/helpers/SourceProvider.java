package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers;

import org.dspace.xoai.serviceprovider.ServiceProvider;
import org.dspace.xoai.serviceprovider.client.HttpOAIClient;
import org.dspace.xoai.serviceprovider.model.Context;

import java.io.Serializable;

public class SourceProvider implements Serializable {
    private static final long serialVersionUID = 1L;

    public ServiceProvider provide(String baseUrl) {
        return new ServiceProvider(new Context().withOAIClient(new HttpOAIClient(baseUrl)));
    }
}
