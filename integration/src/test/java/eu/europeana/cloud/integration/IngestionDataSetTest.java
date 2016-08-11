/* IngestionDataSetTest.java - created on Jan 7, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;

/**
 * This class test integration concerning operation on dataset basis. It tests the whole workflow
 * concerning ingestion of a dataset as well as reading it.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 7, 2014
 */
public abstract class IngestionDataSetTest {
    /**
     * service to manipulate data sets
     */
//  @Autowired
    protected DataSetService          dataSetService;
    /**
     * service that deals with records
     */
//    @Autowired
    protected RecordService           recordService;
    /**
     * service that manages providers
     */
//   @Autowired
    protected DataProviderService     dataProviderService;
    /**
     * service to provide cloud identifiers
     */
//    @Autowired
    protected UniqueIdentifierService uniqueIdentifierService;

    /**
     * Ingestion of a data set and reading the results.
     * 
     * @throws Exception
     */
    @Ignore
    @Test
    public void ingestDataSet() throws Exception {
    	
    	
        String providerId = "test-provider";
        dataProviderService.createProvider(providerId, new DataProviderProperties("Test", "", "",
                "", "", "", "Markus Muhr", ""));

        String dataSetId = "test-dataset";
        dataSetService.createDataSet(providerId, dataSetId, "testing");

        int recordsNumber = 100;
        String schema = "dummy";
        String localIdPrefix = "local-id-";
        String fileNamePrefix = "record-";
        String content = "Randaosdfasdasdlk;fajs a;skdfja;sdfq2p4ir =asdfkla;sjdf 0as;dif 1902-e31ujasd;lf a 0`19u34- as;lkndf ad01-92 3";

        // ingest data set
        for (int i = 0; i < recordsNumber; i++) {
            ByteArrayInputStream is = new ByteArrayInputStream(content.getBytes());

            File file = new File();
            file.setFileName(fileNamePrefix + i);
            file.setMimeType("text");

            CloudId cloudId = uniqueIdentifierService.createCloudId(providerId, localIdPrefix + i);

            Representation represantation = recordService.createRepresentation(cloudId.getId(),
                    schema, providerId);
            recordService.putContent(cloudId.getId(), represantation.getRepresentationName(),
                    represantation.getVersion(), file, is);
            recordService.persistRepresentation(cloudId.getId(), schema,
                    represantation.getVersion());

            dataSetService.addAssignment(providerId, dataSetId, represantation.getCloudId(),
                    represantation.getRepresentationName(), represantation.getVersion());

            is.close();
        }

        // read data set
        ResultSlice<Representation> dataset = dataSetService.listDataSet(providerId, dataSetId,
                null, recordsNumber);
        Assert.assertEquals(recordsNumber, dataset.getResults().size());
        for (Representation representation : dataset.getResults()) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();

            Assert.assertEquals(1, representation.getFiles().size());
            recordService.getContent(representation.getCloudId(), representation.getRepresentationName(),
                    representation.getVersion(), representation.getFiles().get(0).getFileName(), os);

            byte[] readBytes = os.toByteArray();
            Assert.assertEquals(content, new String(readBytes));

            os.close();
        }

        // delete data set
        for (Representation representation : dataset.getResults()) {
            recordService.deleteRepresentation(representation.getCloudId(),
                    representation.getRepresentationName());
            boolean notExists = false;
            try {
                recordService.getRepresentation(representation.getCloudId(),
                        representation.getRepresentationName());
            } catch (RepresentationNotExistsException e) {
                notExists = true;
            }
            Assert.assertEquals(true, notExists);
            dataSetService.removeAssignment(providerId, dataSetId, representation.getCloudId(),
                    representation.getRepresentationName(), representation.getVersion());
        }
        dataset = dataSetService.listDataSet(providerId, dataSetId, null, 1);
        Assert.assertEquals(0, dataset.getResults().size());
        dataSetService.deleteDataSet(providerId, dataSetId);
        ResultSlice<DataSet> dataSets = dataSetService.getDataSets(providerId, null, 1);
        Assert.assertEquals(0, dataSets.getResults().size());
    }
}
