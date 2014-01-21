/* IngestionDataSetTest.java - created on Jan 7, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import java.util.List;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryContentDAO;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryDataSetDAO;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryDataSetService;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryRecordDAO;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryRecordService;
import eu.europeana.cloud.service.uis.InMemoryDataProviderService;
import eu.europeana.cloud.service.uis.InMemoryUniqueIdentifierService;
import eu.europeana.cloud.service.uis.dao.InMemoryCloudIdDao;
import eu.europeana.cloud.service.uis.dao.InMemoryDataProviderDAO;
import eu.europeana.cloud.service.uis.dao.InMemoryLocalIdDao;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;

/**
 * This class test integration concerning operation on dataset basis. It tests the whole workflow
 * concerning ingestion of whole datasets as well as reading them.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 7, 2014
 */
public class InMemoryIngestionDataSetTest extends AbstractIngestionDataSetTest {
    @Override
    public void setupServices() throws Exception {
        InMemoryDataProviderDAO dataProviderDao = new InMemoryDataProviderDAO();
        InMemoryCloudIdDao cloudIdDao = new InMemoryCloudIdDao();
        InMemoryLocalIdDao localIdDao = new InMemoryLocalIdDao();

        dataProviderService = new InMemoryDataProviderService(dataProviderDao);
        uniqueIdentifierService = new InMemoryUniqueIdentifierService(cloudIdDao, localIdDao);

        InMemoryDataSetDAO dataSetDAO = new InMemoryDataSetDAO();
        InMemoryRecordDAO recordDAO = new InMemoryRecordDAO();
        InMemoryContentDAO contentDAO = new InMemoryContentDAO();

        UISClientHandler uisClient = new UISClientHandler() {
            @Override
            public boolean recordExistInUIS(String cloudId) {
                boolean exists = false;
                try {
                    List<CloudId> ids = uniqueIdentifierService.getLocalIdsByCloudId(cloudId);
                    exists = ids != null && !ids.isEmpty();
                } catch (DatabaseConnectionException | CloudIdDoesNotExistException
                        | ProviderDoesNotExistException | RecordDatasetEmptyException e) {
                    // ignore
                }
                return exists;
            }

            @Override
            public boolean providerExistsInUIS(String providerId) {
                boolean exists = false;
                try {
                    DataProvider provider = dataProviderService.getProvider(providerId);
                    exists = provider != null;
                } catch (ProviderDoesNotExistException e) {
                    // ignore
                }
                return exists;
            }
        };

        dataSetService = new InMemoryDataSetService(dataSetDAO, recordDAO, uisClient);
        recordService = new InMemoryRecordService(recordDAO, contentDAO, dataSetDAO, uisClient);
    }
}
