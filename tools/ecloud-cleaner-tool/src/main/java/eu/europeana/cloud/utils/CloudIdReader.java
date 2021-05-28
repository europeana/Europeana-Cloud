package eu.europeana.cloud.utils;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

public class CloudIdReader {
    private String filename;
    private LineNumberReader reader = null;

    public CloudIdReader(String filename) {
        this.filename = filename;
    }

    public List<String> getNextCloudId(int fetchSize) throws IOException {
        if(reader == null) {
            reader = new LineNumberReader(new FileReader(filename));
        }

        List<String> result = new ArrayList<>(fetchSize);
        String cloudId;
        do {
            cloudId = reader.readLine();
            if(cloudId != null) {
                result.add(cloudId);
            }
            fetchSize--;
        } while(fetchSize > 0 && cloudId != null);

        return result;
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
