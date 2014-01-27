/* IngestionTool.java - created on Jan 20, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;

/**
 * This is a command line tool to ingest a new data set.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 20, 2014
 */
@Component
public class ServiceIngestionTool {
    @Autowired
    private DataSetService          dataSetService;

    @Autowired
    private RecordService           recordService;

    @Autowired
    private DataProviderService     dataProviderService;

    @Autowired
    private UniqueIdentifierService uniqueIdentifierService;

    /**
     * Available operations for this tool.
     * 
     * @author Markus Muhr (markus.muhr@kb.nl)
     * @since Jan 19, 2012
     */
    public enum Operation {
        /**
         * ingests data set
         */
        ingestDataSet(
                      "Ingests a data set for a given provider-id, dataset-id, schema and directory!"),
        /**
         * reads data set
         */
        readDataSet("Reads a data set for a given provider-id, dataset-id, schema and directory!"),
        /**
         * deletes data set
         */
        deleteDataSet(
                      "Deletes a data set for a given provider-id, dataset-id, schema and directory!");

        /**
         * description for option
         */
        private String description;

        /**
         * Creates a new instance of this class.
         * 
         * @param description
         */
        private Operation(String description) {
            this.description = description;
        }

        /**
         * @return description for option
         */
        public String getDescription() {
            return description;
        }
    }

    @Option(name = "-o", aliases = { "--operation" }, required = true)
    private Operation operation;

    @Option(name = "-p", aliases = { "--provider" }, usage = "Provider for which a dataset should be ingested")
    private String    providerId;

    @Option(name = "-d", aliases = { "--dataset" }, usage = "Dataset for which the data should be ingested")
    private String    dataSetId;

    @Option(name = "-s", aliases = { "--schema" }, usage = "Schema specifying the format of the dataset!")
    private String    schema;

    @Option(name = "-f", aliases = { "--file" }, metaVar = "DIR", usage = "Directory with files to be ingested!")
    private String    directory;

    /**
     * Runs operations for this tool.
     */
    private void run() {
        switch (operation) {
        case ingestDataSet:
            try {
                ingestDataSet();
            } catch (Exception e) {
                throw new RuntimeException("Could not ingest dataset!", e);
            }
            break;
        case readDataSet:
            try {
                readDataSet();
            } catch (Exception e) {
                throw new RuntimeException("Could not read dataset!", e);
            }
            break;
        case deleteDataSet:
            try {
                deleteDataSet();
            } catch (Exception e) {
                throw new RuntimeException("Could not delete dataset!", e);
            }
            break;
        }
    }

    /**
     * Ingestion of a data set.
     * 
     * @throws Exception
     */
    private void ingestDataSet() throws Exception {
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
            FileInputStream is = new FileInputStream(input);

            File file = new File();
            file.setFileName(input.getName());
            file.setMimeType("text");

            CloudId cloudId = uniqueIdentifierService.createCloudId(providerId, input.getName());

            Representation represantation = recordService.createRepresentation(cloudId.getId(),
                    schema, providerId);
            recordService.putContent(cloudId.getId(), represantation.getSchema(),
                    represantation.getVersion(), file, is);
            recordService.persistRepresentation(cloudId.getId(), schema,
                    represantation.getVersion());

            dataSetService.addAssignment(providerId, dataSetId, represantation.getRecordId(),
                    represantation.getSchema(), represantation.getVersion());

            is.close();
        }
    }

    /**
     * Reads a data set to a directory!
     * 
     * @throws Exception
     */
    private void readDataSet() throws Exception {
        ResultSlice<Representation> dataset = dataSetService.listDataSet(providerId, dataSetId,
                null, Integer.MAX_VALUE);
        for (Representation representation : dataset.getResults()) {
            FileOutputStream os = new FileOutputStream(new java.io.File(
                    directory + "/" + representation.getFiles().get(0).getFileName()));
            recordService.getContent(representation.getRecordId(), representation.getSchema(),
                    representation.getVersion(), representation.getFiles().get(0).getFileName(), os);
            os.close();
        }
    }

    /**
     * Delete a data set!
     * 
     * @throws Exception
     */
    private void deleteDataSet() throws Exception {
        ResultSlice<Representation> dataset = dataSetService.listDataSet(providerId, dataSetId,
                null, Integer.MAX_VALUE);
        for (Representation representation : dataset.getResults()) {
            recordService.deleteRepresentation(representation.getRecordId(),
                    representation.getSchema());
            dataSetService.removeAssignment(providerId, dataSetId, representation.getRecordId(),
                    representation.getSchema());
        }
        dataSetService.deleteDataSet(providerId, dataSetId);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
                "inmemoryIngestionToolContext.xml");
        ServiceIngestionTool tool = context.getBean(ServiceIngestionTool.class);

// IngestionTool tool = new IngestionTool();
        CmdLineParser parser = new CmdLineParser(tool);
        try {
            parser.parseArgument(args);
            tool.run();
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }

        context.close();
    }
}
