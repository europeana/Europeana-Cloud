package eu.europeana.cloud.migrator;

import eu.europeana.cloud.common.model.DataProviderProperties;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.*;

public class EuropeanaNewspapersResourceProvider
        extends DefaultResourceProvider {

    public static final String IMAGE_DIR = "image";

    private Map<String, String> reversedMapping = new HashMap<String, String>();

    private Map<String, String> duplicateMapping = new HashMap<String, String>();

    private Map<String, Integer> fileCounts = new HashMap<String, Integer>();

    private static final Logger logger = Logger.getLogger(EuropeanaNewspapersResourceProvider.class);

    public EuropeanaNewspapersResourceProvider(String representationName, String mappingFile, String locations) throws IOException {
        super(representationName, mappingFile, locations);
        readMappingFile();
    }

    /**
     * Reads mapping file given while constructing this object.
     * File must be a csv file with ; delimited lists of local identifier and paths to files of the issue.
     * Encoding is UTF-8.
     */
    private void readMappingFile() throws IOException {
        Path mappingPath = null;
        try {
            // try to treat the mapping file as local file
            mappingPath = FileSystems.getDefault().getPath(".", mappingFile);
        } catch (InvalidPathException e) {
            // in case path cannot be created try to treat the mapping file as absolute path
            mappingPath = FileSystems.getDefault().getPath(mappingFile);
        }
        if (mappingPath == null || !mappingPath.toFile().exists())
            throw new IOException("Mapping file cannot be found: " + mappingFile);

        String localId;
        String path;
        List<String> paths = new ArrayList<String>();
        BufferedReader reader = null;

        try {
            reader = Files.newBufferedReader(mappingPath, Charset.forName("UTF-8"));
            for (; ; ) {
                String line = reader.readLine();
                if (line == null)
                    break;
                StringTokenizer tokenizer = new StringTokenizer(line, ";");
                // first token is local identifier
                if (tokenizer.hasMoreTokens())
                    localId = tokenizer.nextToken().trim();
                else
                    localId = null;
                if (localId == null) {
                    logger.warn("Local identifier is null (" + localId + "). Skipping line.");
                    continue;
                }

                boolean duplicate = false;

                paths.clear();
                int count = 0;

                while (tokenizer.hasMoreTokens()) {
                    path = tokenizer.nextToken().trim();
                    // when path is empty do not add to map
                    if (path.isEmpty())
                        continue;
                    if (reversedMapping.get(path) != null && !duplicate) {
                        logger.warn("File " + path + " already has a local id = " + reversedMapping.get(path) + ". New local id = " + localId);
                        duplicate = true;
                        for (String s : paths) {
                            reversedMapping.remove(s);
                            duplicateMapping.put(s, localId);
                        }
                    }
                    if (duplicate)
                        // add reversed mapping to duplicates map
                        duplicateMapping.put(path, localId);
                    else {
                        // add reversed mapping
                        reversedMapping.put(path, localId);
                        paths.add(path);
                    }
                    count++;
                }
                fileCounts.put(localId, Integer.valueOf(count));
            }
        } finally {
            if (reader != null)
                reader.close();
        }
    }

    @Override
    public String getProviderId(String path) {
        if (!path.contains(IMAGE_DIR)) {
            logger.error("No image directory found in resource path.");
            return null;
        }

        int pos = path.indexOf(IMAGE_DIR);
        String rest = path.substring(pos + IMAGE_DIR.length());
        if (rest.startsWith(ResourceMigrator.LINUX_SEPARATOR) || rest.startsWith(ResourceMigrator.WINDOWS_SEPARATOR))
            rest = rest.substring(1);
        pos = rest.indexOf(ResourceMigrator.LINUX_SEPARATOR);
        if (pos == -1)
            pos = rest.indexOf(ResourceMigrator.WINDOWS_SEPARATOR);

        return rest.substring(0, pos > -1 ? pos : rest.length());
    }

    @Override
    public DataProviderProperties getDataProviderProperties(String path) {
        // get provider name from path
        String id = getProviderId(path);
        // create directory object for provider
        File f = new File(path.substring(0, path.indexOf(id) + id.length()));
        if (!f.isDirectory())
            return getDefaultDataProviderProperties();

        // assume we can find a file with data provider properties named id.properties
        File dpFile = new File(f, id + PROPERTIES_EXTENSION);
        if (!dpFile.exists())
            return getDefaultDataProviderProperties();
        return getDataProviderPropertiesFromFile(dpFile);
    }

    @Override
    public String getLocalIdentifier(String location, String path, boolean duplicate) {
        // first get the local path within location
        String localPath = getLocalPath(location, path);
        // we have to find the identifier in the mapping file
        String localId = duplicate ? duplicateMapping.get(localPath) : reversedMapping.get(localPath);
        if (localId == null)
            logger.warn("Local identifier for file " + localPath + " was not found in the mapping file!" + (duplicate ? " Duplicate not present also." : ""));
        return localId;
    }

    private String getLocalPath(String location, String path) {
        int i = path.indexOf(location);
        if (i == -1)
            return path;
        return path.substring(i + location.length() + 1);
    }

    @Override
    public int getFileCount(String localId) {
        Integer count = fileCounts.get(localId);
        if (count == null)
            return -1;
        return count.intValue();
    }
}
