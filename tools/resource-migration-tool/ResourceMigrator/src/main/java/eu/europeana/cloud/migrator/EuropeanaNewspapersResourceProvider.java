package eu.europeana.cloud.migrator;

import eu.europeana.cloud.common.model.DataProviderProperties;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class EuropeanaNewspapersResourceProvider
        extends DefaultResourceProvider {
    private Map<String, String> mapping = new HashMap<String, String>();

    private Map<String, String> reversedMapping = new HashMap<String, String>();

    private static final Logger logger = Logger.getLogger(EuropeanaNewspapersResourceProvider.class);

    public EuropeanaNewspapersResourceProvider(String representationName, String mappingFile, String locations) {
        super(representationName, mappingFile, locations);
    }

    /**
     * Reads mapping file given while constructing this object.
     * File must be a csv file with ; delimited pairs of local identifier and path to issue.
     * Encoding is UTF-8.
     */
    private void readMappingFile() {
        File f = new File(mappingFile);
        BufferedReader br = null;

        try {
            br = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(f), "UTF8"));
            String line;
            String localId;
            String path;

            while ((line = br.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line, ";");
                if (tokenizer.hasMoreTokens())
                    localId = tokenizer.nextToken();
                else
                    localId = null;
                if (tokenizer.hasMoreTokens())
                    path = tokenizer.nextToken();
                else
                    path = null;

                if (localId == null || path == null) {
                    logger.warn("Either local identifier or path is null (" + localId + " , " + path + "). Skipping line.");
                    continue;
                }

                // add mapping
                mapping.put(localId, path);
                // add reversed mapping
                reversedMapping.put(path, localId);
            }
        } catch (FileNotFoundException e) {
            logger.error("File not found " + mappingFile, e);
        } catch (UnsupportedEncodingException e) {
            logger.error("File " + mappingFile + " is not UTF-8 encoded.", e);
        } catch (IOException e) {
            logger.error("Problem reading file " + mappingFile, e);
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                logger.error("Could not close reader for file " + mappingFile, e);
            }
        }
    }

    @Override
    public void migrate() {
        // read mapping file and prepare mapping and reversed mapping
        readMappingFile();
    }

    @Override
    public String getProviderId(String path) {
        return null;
    }

    @Override
    public DataProviderProperties getDataProviderProperties(String path) {
        return null;
    }

    @Override
    public String getLocalIdentifier(String path, String providerId) {
        return null;
    }
}
