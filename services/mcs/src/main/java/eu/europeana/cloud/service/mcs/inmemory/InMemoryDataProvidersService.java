package eu.europeana.cloud.service.mcs.inmemory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;
import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.DataProviderService;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.ProviderAlreadyExistsException;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryDataProvidersService implements DataProviderService {

    @Autowired
    private InMemoryDataSetDAO dataSetDAO;

    @Autowired
    private InMemoryDataProviderDAO dataProviderDAO;


    @Override
    public List<DataProvider> getProviders() {
        return dataProviderDAO.getProviders();
    }


    @Override
    public DataProvider getProvider(String id)
            throws ProviderNotExistsException {
        return dataProviderDAO.getProvider(id);
    }


    @Override
    public DataProvider createProvider(String providerId, DataProviderProperties properties) {
        return dataProviderDAO.createProvider(providerId, properties);
    }


    @Override
    public void deleteProvider(String providerId)
            throws ProviderNotExistsException,
            ProviderHasDataSetsException, ProviderHasRecordsException {
        List<DataSet> providerDataSets = dataSetDAO.getDataSets(providerId);
        if (providerDataSets != null && !providerDataSets.isEmpty()) {
            throw new ProviderHasDataSetsException();
        }
        dataProviderDAO.deleteProvider(providerId);
    }
}
