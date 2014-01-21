/* IngestionDataSetTest.java - created on Jan 7, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This class test integration concerning operation on dataset basis. It tests the whole workflow
 * concerning ingestion of whole datasets as well as reading them.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 7, 2014
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/inmemoryServicesTestContext.xml" })
public class InMemoryIngestionDataSetTest extends IngestionDataSetTest {
//    @Override
//    public void setupServices() throws Exception {
//        InMemoryDataProviderDAO dataProviderDao = new InMemoryDataProviderDAO();
//        InMemoryCloudIdDao cloudIdDao = new InMemoryCloudIdDao();
//        InMemoryLocalIdDao localIdDao = new InMemoryLocalIdDao();
//
//        dataProviderService = new InMemoryDataProviderService(dataProviderDao);
//        uniqueIdentifierService = new InMemoryUniqueIdentifierService(cloudIdDao, localIdDao);
//
//        InMemoryDataSetDAO dataSetDAO = new InMemoryDataSetDAO();
//        InMemoryRecordDAO recordDAO = new InMemoryRecordDAO();
//        InMemoryContentDAO contentDAO = new InMemoryContentDAO();
//
//        UISClientHandler uisClient = new UISClientHandler() {
//            @Override
//            public boolean recordExistInUIS(String cloudId) {
//                boolean exists = false;
//                try {
//                    List<CloudId> ids = uniqueIdentifierService.getLocalIdsByCloudId(cloudId);
//                    exists = ids != null && !ids.isEmpty();
//                } catch (DatabaseConnectionException | CloudIdDoesNotExistException
//                        | ProviderDoesNotExistException | RecordDatasetEmptyException e) {
//                    // ignore
//                }
//                return exists;
//            }
//
//            @Override
//            public boolean providerExistsInUIS(String providerId) {
//                boolean exists = false;
//                try {
//                    DataProvider provider = dataProviderService.getProvider(providerId);
//                    exists = provider != null;
//                } catch (ProviderDoesNotExistException e) {
//                    // ignore
//                }
//                return exists;
//            }
//        };
//
//        dataSetService = new InMemoryDataSetService(dataSetDAO, recordDAO, uisClient);
//        recordService = new InMemoryRecordService(recordDAO, contentDAO, dataSetDAO, uisClient);
//    }
}
