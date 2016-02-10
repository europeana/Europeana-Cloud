package eu.europeana.cloud.migrator;

import eu.europeana.cloud.common.model.DataProviderProperties;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class EuropeanaNewspapersResourceProvider
        extends DefaultResourceProvider {
    public static final String IMAGE_DIR = "image";
    private Map<String, String> mapping = new HashMap<String, String>();

    private Map<String, String> reversedMapping = new HashMap<String, String>();

    private static final Logger logger = Logger.getLogger(EuropeanaNewspapersResourceProvider.class);

    public EuropeanaNewspapersResourceProvider(String representationName, String mappingFile, String locations) {
        super(representationName, mappingFile, locations);
        readMappingFile();
    }

    /**
     * Reads mapping file given while constructing this object.
     * File must be a csv file with ; delimited pairs of local identifier and path to issue.
     * Encoding is UTF-8.
     */
    private void readMappingFile() {
        try {
            Path mappingPath = null;
            try {
                // try to treat the mapping file as local file
                mappingPath = FileSystems.getDefault().getPath(".", mappingFile);
            } catch (InvalidPathException e) {
                // in case path cannot be created try to treat the mapping file as absolute path
                mappingPath = FileSystems.getDefault().getPath(mappingFile);
            }
            if (!mappingPath.toFile().exists()) {
                logger.warn("Mapping file cannot be found: " + mappingFile + ".\nMapping will not be used!");
                return;
            }

            String localId;
            String path;

            for (String line : Files.readAllLines(mappingPath, Charset.forName("UTF-8"))) {
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
                if (reversedMapping.get(path) != null)
                    logger.warn("Path " + path + " already has a local id = " + reversedMapping.get(path));
                // add reversed mapping
                reversedMapping.put(path, localId);
            }
        } catch (IOException e) {
            logger.warn("Problem occurred when reading mapping file!", e);
        }
    }

    @Override
    public void migrate() {
        // read mapping file and prepare mapping and reversed mapping
        readMappingFile();
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
    public String getLocalIdentifier(String location, String path) {
        // first get the local path within location
        String localPath = getLocalPath(location, path);
        // we have to find the identifier in the mapping file
        String localId = reversedMapping.get(localPath);
        if (localId == null)
            logger.warn("Local identifier for file " + path + " was not found in the mapping file!");
        return localId;
    }

    private String getLocalPath(String location, String path) {
        int i = path.indexOf(location);
        if (i == -1)
            return path;
        String local = path.substring(i + location.length() + 1);
        i = local.lastIndexOf("/");
        if (i == -1)
            i = path.lastIndexOf("\\");
        // when there are no slashes or backslashes it means that the local path is just a filename
        if (i == -1)
            return local;
        return local.substring(0, i);
    }


    private String getIssue(String path, String dataProvider) {
        int i = path.indexOf(dataProvider);
        if (i == -1)
            return null;
        i += dataProvider.length() + 1;
        int j = path.lastIndexOf("/");
        if (j == -1)
            j = path.lastIndexOf("\\");
        if (j == -1)
            j = path.length();
        return path.substring(i, j);
    }
}
