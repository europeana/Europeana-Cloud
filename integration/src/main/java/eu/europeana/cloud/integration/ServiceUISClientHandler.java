/* ServiceUISClientHandler.java - created on Jan 21, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;

/**
 * This handler uses services to perform existance tests on the UIS.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 21, 2014
 */
public class ServiceUISClientHandler implements UISClientHandler {

    @Autowired
    private DataProviderService dataProviderService;

    @Autowired
    private UniqueIdentifierService uniqueIdentifierService;

    @Override
    public boolean existsCloudId(String cloudId) {
        boolean exists = false;
        try {
            List<CloudId> ids = uniqueIdentifierService.getLocalIdsByCloudId(cloudId);
            exists = ids != null && !ids.isEmpty();
        } catch (DatabaseConnectionException | CloudIdDoesNotExistException | ProviderDoesNotExistException
                | RecordDatasetEmptyException e) {
            // ignore
        }
        return exists;
    }

    @Override
    public DataProvider getProvider(String providerId) {
	DataProvider result = null;
	try {
	    result = dataProviderService.getProvider(providerId);
	} catch (ProviderDoesNotExistException e) {
	    // ignore
	}
	return result;
    }
    
    @Override
    public boolean existsProvider(String providerId) {
	DataProvider result = null;
	try {
	    result = dataProviderService.getProvider(providerId);
	} catch (ProviderDoesNotExistException e) {
	    // ignore
	}
	return result != null;
    }
}
