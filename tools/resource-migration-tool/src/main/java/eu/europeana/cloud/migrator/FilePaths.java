package eu.europeana.cloud.migrator;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by helin on 2016-02-09.
 */
public class FilePaths {

    /**
     * Location part in path
     */
    private String location;

    /**
     * Data provider
     */
    private String dataProvider;


    private List<String> paths;

    public FilePaths(String location, String dataProvider) {
        this.location = location;
        this.dataProvider = dataProvider;
        this.paths = new ArrayList<String>();
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
        return paths.size();
    }
}
