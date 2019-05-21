package eu.europeana.cloud.migrator.provider;

import eu.europeana.cloud.migrator.ResourceMigrator;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by helin on 2016-02-09.
 */
public class FilePaths {

    private static final String prefix = "paths_";
    private static final Logger logger = Logger.getLogger(FilePaths.class);

    /**
     * Location part in path
     */
    private String location;

    /**
     * Data provider
     */
    private String dataProvider;


    /**
     * Identifier
     */
    private String identifier;

    private List<String> paths;

    /**
     * When true fileName is used to determine the file where paths are stored instead of paths list
     */
    private boolean useFile = false;

    /**
     * Filename to store paths, NOT absolute path, just file name
     */
    private String fileName = null;

    /**
     * Store size of paths in case of using file
     */
    private int size = 0;

    public FilePaths(String location, String dataProvider) {
        this.location = location;
        this.dataProvider = dataProvider;
        this.paths = new ArrayList<String>();
        this.identifier = null;
    }

    public void useFile(String file) {
        useFile = true;
        fileName = file;
        clean();
    }

    private void clean() {
        if (!useFile || fileName == null)
            return;

        Path dest = FileSystems.getDefault().getPath(".", prefix + fileName + ResourceMigrator.TEXT_EXTENSION);
        try {
            if (dest.toFile().exists())
                Files.write(dest, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            logger.error(e.getMessage() + " .Because of " + e.getCause());
        }
    }


    public List<String> getFullPaths() {
        return paths;
    }


    public String getLocation() {
        return location;
    }

    public String getDataProvider() {
        return dataProvider;
    }

    public int size() {
        if (useFile)
            return size;
        return paths.size();
    }

    public void setIdentifier(String id) {
        identifier = id;
    }

    public String getIdentifier() {
        if (identifier == null)
            return dataProvider;
        return identifier;
    }

    public void addPaths(List<String> pathsToAdd) {
        if (paths == null)
            return;

        for (String path : pathsToAdd)
            addPath(path);
    }

    public void addPath(String path) {
        if (useFile && fileName != null)
            addPathToFile(path);
        else
            paths.add(path);
    }

    private void addPathToFile(String path) {
        try {
            Path dest = FileSystems.getDefault().getPath(".", prefix + fileName + ResourceMigrator.TEXT_EXTENSION);
            Files.write(dest, String.valueOf(path + "\n").getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
            size++;
        } catch (IOException e) {
            System.out.println("Cannot store path " + path + "in file " + prefix + fileName + ResourceMigrator.TEXT_EXTENSION);
            logger.error("Cannot store path " + path + "in file " + prefix + fileName + ResourceMigrator.TEXT_EXTENSION);
        }
    }

    public BufferedReader getPathsReader() {
        if (useFile && fileName != null) {
            try {
                return Files.newBufferedReader(FileSystems.getDefault().getPath(".", prefix + fileName + ResourceMigrator.TEXT_EXTENSION), Charset.forName("UTF-8"));
            } catch (IOException e) {
                logger.error("Error while reading a file", e);
            }
        }
        return null;
    }

    public void sort() {
        if (!useFile && paths.size() > 0) {
            Collections.sort(paths);
        }
    }

    public void removeAll(List<String> processed) {
        BufferedReader reader = getPathsReader();
        if (reader == null)
            paths.removeAll(processed);
        else {
            Path dest = FileSystems.getDefault().getPath(".", prefix + fileName + ".tmp");
            try {
                for (; ; ) {
                    String line = reader.readLine();
                    if (line == null)
                        break;
                    if (processed.contains(line)) {
                        size--;
                        continue;
                    }
                    Files.write(dest, String.valueOf(line + "\n").getBytes(Charset.forName("UTF-8")), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
                }
            } catch (IOException e) {
                // do nothing, all paths will be used
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        logger.error("Error while closing an open reader ", e);
                    }
                }
            }
            Path finalDest = FileSystems.getDefault().getPath(".", prefix + fileName + ResourceMigrator.TEXT_EXTENSION);
            try {
                if (dest.toFile().exists())
                    Files.move(dest, finalDest, StandardCopyOption.REPLACE_EXISTING);
                else
                    Files.write(finalDest, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
                if (size < 0)
                    size = 0;
            } catch (IOException e) {
                logger.error("Error while removing/writing a file ", e);
            }
        }
    }
}
