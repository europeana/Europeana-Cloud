/* IngestionTool.java - created on Jan 20, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import java.io.FileInputStream;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.uis.CassandraDataProviderService;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.PersistentUniqueIdentifierService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;

/**
 * This is a command line tool to ingest a new data set.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 20, 2014
 */
public class IngestionTool {
    @Option(name = "-p", aliases = { "--provider" }, usage = "Provider for which a dataset should be ingested")
    private String providerId;

    @Option(name = "-d", aliases = { "--dataset" }, usage = "Dataset for which the data should be ingested")
    private String dataSetId;

    @Option(name = "-s", aliases = { "--schema" }, usage = "Schema specifying the format of the dataset!")
    private String schema;

    @Option(name = "-d", aliases = { "--directory" }, metaVar = "DIR", usage = "Directory with files to be ingested!")
    private String directory;

    /**
     * Ingestion of a data set.
     * 
     * @throws Exception
     */
    public void ingest() throws Exception {
        DataSetService dataSetService = new CassandraDataSetService();
        RecordService recordService = new CassandraRecordService();
        DataProviderService dataProviderService = new CassandraDataProviderService();
        UniqueIdentifierService uniqueIdentifierService = new PersistentUniqueIdentifierService(
                null, null, null);

        try {
            dataProviderService.createProvider(providerId, new DataProviderProperties(providerId,
                    "", "", "", "", "", "Ingestion Tool Provide4", ""));
        } catch (ProviderAlreadyExistsException e) {
            // ignore if provider exists
        }

        try {
            dataSetService.createDataSet(providerId, dataSetId, "Ingestion Tool Dataset");
        } catch (ProviderNotExistsException e) {
            throw e;
        } catch (DataSetAlreadyExistsException e) {
            // ignore if data set exists
        }

        Iterator<java.io.File> iter = FileUtils.iterateFiles(new java.io.File(directory), null,
                true);
        while (iter.hasNext()) {
            java.io.File input = iter.next();

            File file = new File();
            file.setFileName(input.getName());
            file.setMimeType("text");

            CloudId cloudId = uniqueIdentifierService.createCloudId(providerId, input.getName());

            Representation represantation = recordService.createRepresentation(cloudId.getId(),
                    schema, providerId);
            recordService.putContent(cloudId.getId(), represantation.getSchema(),
                    represantation.getVersion(), file, new FileInputStream(input));
            recordService.persistRepresentation(cloudId.getId(), schema,
                    represantation.getVersion());

            dataSetService.addAssignment(providerId, dataSetId, represantation.getRecordId(),
                    represantation.getSchema(), represantation.getVersion());
        }
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        IngestionTool tool = new IngestionTool();
        CmdLineParser parser = new CmdLineParser(tool);
        try {
            parser.parseArgument(args);
            tool.ingest();
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }
    }
}
