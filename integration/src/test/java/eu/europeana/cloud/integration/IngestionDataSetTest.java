/* IngestionDataSetTest.java - created on Jan 7, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import java.io.ByteArrayInputStream;

import org.junit.Before;
import org.junit.Test;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryContentDAO;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryDataSetDAO;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryDataSetService;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryRecordDAO;
import eu.europeana.cloud.service.mcs.inmemory.InMemoryRecordService;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.InMemoryDataProviderService;
import eu.europeana.cloud.service.uis.InMemoryUniqueIdentifierService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.dao.InMemoryCloudIdDao;
import eu.europeana.cloud.service.uis.dao.InMemoryDataProviderDAO;
import eu.europeana.cloud.service.uis.dao.InMemoryLocalIdDao;

/**
 * This class test integration concerning operation on dataset basis. It tests the whole workflow
 * concerning ingestion of whole datasets as well as reading them.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 7, 2014
 */
public class IngestionDataSetTest {
    private DataSetService          dataSetService;
    private RecordService           recordService;
    private DataProviderService     dataProviderService;
    private UniqueIdentifierService uniqueIdentifierService;

    /**
     * Initializes services.
     * 
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        InMemoryDataProviderDAO dataProviderDao = new InMemoryDataProviderDAO();
        InMemoryDataSetDAO dataSetDAO = new InMemoryDataSetDAO();
        InMemoryRecordDAO recordDAO = new InMemoryRecordDAO();
        InMemoryContentDAO contentDAO = new InMemoryContentDAO();
        InMemoryCloudIdDao cloudIdDao = new InMemoryCloudIdDao();
        InMemoryLocalIdDao localIdDao = new InMemoryLocalIdDao();

        dataSetService = new InMemoryDataSetService(dataSetDAO, recordDAO, dataProviderDao);
        recordService = new InMemoryRecordService(recordDAO, contentDAO, dataSetDAO,
                dataProviderDao);
        dataProviderService = new InMemoryDataProviderService(dataProviderDao);
        uniqueIdentifierService = new InMemoryUniqueIdentifierService(cloudIdDao, localIdDao);
    }

    /**
     * Ingestion of a data set and reading the results.
     * 
     * @throws Exception
     */
    @Test
    public void ingestDataSet() throws Exception {
        String providerId = "test-provider";
        dataProviderService.createProvider(providerId, new DataProviderProperties("Test", "", "",
                "", "", "", "Markus Muhr", ""));

        String dataSetId = "test-dataset";
        dataSetService.createDataSet(providerId, dataSetId, "testing");

        String schema = "dummy";
        String localIdPrefix = "local-id-";
        String fileNamePrefix = "record-";
        String content = "Randaosdfasdasdlk;fajs a;skdfja;sdfq2p4ir =asdfkla;sjdf 0as;dif 1902-e31ujasd;lf a 0`19u34- as;lkndf ad01-92 3";
        for (int i = 0; i < 1000; i++) {
            File file = new File();
            file.setFileName(fileNamePrefix + i);
            file.setMimeType("text");

            CloudId cloudId = uniqueIdentifierService.createCloudId(providerId, localIdPrefix + i);
            Representation represantation = recordService.createRepresentation(cloudId.getId(),
                    schema, providerId);
            recordService.putContent(cloudId.getId(), represantation.getSchema(),
                    represantation.getVersion(), file, new ByteArrayInputStream(content.getBytes()));
            recordService.persistRepresentation(cloudId.getId(), schema,
                    represantation.getVersion());

            dataSetService.addAssignment(providerId, dataSetId, represantation.getRecordId(),
                    represantation.getSchema(), represantation.getVersion());
        }

//        ResultSlice<Representation> dataset = dataSetService.listDataSet(providerId, dataSetId, null, 1000);
//        for (Representation representation : dataset.getResults()) {
//            recordService.getContent(representation.getRecordId(), representation.getSchema(), representation.getVersion(), fileName, os)
//            recordService.getFile(representation.getRecordId(), representation.getSchema(), representation.getVersion(), fileName);
//        }
    }
}
