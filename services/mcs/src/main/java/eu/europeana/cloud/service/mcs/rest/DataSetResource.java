
package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import static eu.europeana.cloud.service.mcs.rest.PathConstants.*;

import eu.europeana.cloud.service.mcs.service.DataProviderService;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}")
@Component
public class DataSetResource {

    private static final Logger log = LoggerFactory.getLogger(DataSetResource.class);
    
    @Autowired
    private DataProviderService providerService;
    
    @PathParam(P_PROVIDER)
    private String providerId;
    
    @PathParam(P_DATASET)
    private String dataSetId;

    @DELETE
    public void deleteDataSet() throws ProviderNotExistsException, DataSetNotExistsException {
        providerService.deleteDataSet(providerId, dataSetId);
    }
}