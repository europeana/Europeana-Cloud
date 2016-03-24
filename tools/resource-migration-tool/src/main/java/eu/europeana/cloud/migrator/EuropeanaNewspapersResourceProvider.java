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

    public EuropeanaNewspapersResourceProvider(String representationName, String mappingFile, String locations, String dataProviderId) throws IOException {
        super(representationName, mappingFile, locations, dataProviderId);
        if (dataProviderId == null)
            throw new IllegalArgumentException("Data provider identifier must be specified for Europeana Newspapers migration!");
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
            if (!mappingPath.toFile().exists())
                mappingPath = FileSystems.getDefault().getPath(mappingFile);
        } catch (InvalidPathException e) {
            // in case path cannot be created try to treat the mapping file as absolute path
            mappingPath = FileSystems.getDefault().getPath(mappingFile);
            logger.info("Invalid Path exception. Mapping file " + mappingFile + " as absolute path: " + mappingPath);
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
    public String getResourceProviderId(String path) {
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
    public String getDataProviderId(String path) {
        // path is not used to determine data provider, always use the configured data provider
        return dataProviderId;
    }

    @Override
    public DataProviderProperties getDataProviderProperties(String path) {
        String id = getDataProviderId(path);
        if (id == null) {
            // something is wrong, data provider not specified, this should never happen
            throw new IllegalArgumentException("Data provider identifier must be specified for Europeana Newspapers migration!");
        }

        File f = new File(path);
        // when file is id.properties return properties from file
        if (f.exists() && f.isFile() && f.getName().equals(id + PROPERTIES_EXTENSION))
            return getDataProviderPropertiesFromFile(f);

        // when file is directory try to search for file id.properties inside
        if (f.isDirectory()) {
            File dpFile = new File(f, id + PROPERTIES_EXTENSION);
            if (dpFile.exists())
                return getDataProviderPropertiesFromFile(dpFile);
        }

        return getDefaultDataProviderProperties();
    }

    @Override
    public String getLocalIdentifier(String location, String path, boolean duplicate) {
        // first get the local path within location
        String localPath = getLocalPath(location, path);
        // we have to find the identifier in the mapping file
        String localId = duplicate ? duplicateMapping.get(localPath) : reversedMapping.get(localPath);
        // when searching in normal mapping and id is not found display a warning
        if (localId == null && !duplicate)
            logger.warn("Local identifier for file " + localPath + " was not found in the mapping file!");
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

    @Override
    public List<FilePaths> split(List<FilePaths> paths) {
        List<FilePaths> result = new ArrayList<FilePaths>();
        for (FilePaths fp : paths) {
            result.addAll(split(fp, true));
        }
        return result;
    }

    private List<FilePaths> split(FilePaths fp, boolean year) {
        // split will be done for every newspaper title which is the directory just inside the provider directory
        List<FilePaths> result = new ArrayList<FilePaths>();
        Map<String, List<String>> titlePaths = new HashMap<String, List<String>>();

        for (String path : fp.getFullPaths()) {
            int i = path.indexOf(fp.getLocation());
            i = path.indexOf(fp.getDataProvider(), i == -1 ? 0 : i + fp.getLocation().length() + 1);
            if (i == -1) {
                // no data provider name in path, strange so return the FilePaths object unchanged regardless the other paths could contain provider name
                result.add(fp);
                return result;
            }
            String title = path.substring(i + fp.getDataProvider().length() + 1);
            i = title.indexOf(ResourceMigrator.LINUX_SEPARATOR);
            if (i == -1) {
                // no directory found in path, strange so return the FilePaths object unchanged regardless the other paths
                result.add(fp);
                return result;
            }
            if (year) {
                // add year to title, for every year of a title there will be a separate thread
                // find next separator
                i = title.indexOf(ResourceMigrator.LINUX_SEPARATOR, i + 1);
            }
            title = title.substring(0, i);
            if (titlePaths.get(title) == null) {
                titlePaths.put(title, new ArrayList<String>());
            }
            titlePaths.get(title).add(path);
        }

        if (titlePaths.size() == 1) {
            // all paths belong to the same title so no need to create a new FilePaths object as it would be the same as the input one
            result.add(fp);
        } else {
            // now create FilePaths object for every newspapers title
            for (Map.Entry<String, List<String>> entry : titlePaths.entrySet()) {
                FilePaths filePaths = new FilePaths(fp.getLocation(), fp.getDataProvider());
                filePaths.getFullPaths().addAll(entry.getValue());
                filePaths.setIdentifier(entry.getKey().replace(ResourceMigrator.LINUX_SEPARATOR, "_"));
                result.add(filePaths);
            }
        }
        return result;
    }
}
