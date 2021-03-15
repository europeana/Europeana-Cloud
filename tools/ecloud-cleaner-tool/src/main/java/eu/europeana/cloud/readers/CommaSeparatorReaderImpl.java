package eu.europeana.cloud.readers;

import eu.europeana.cloud.api.TaskIdsReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tarek on 4/17/2019.
 */
public class CommaSeparatorReaderImpl implements TaskIdsReader {
    private static final String LINE_SEPARATOR = ",";

    @Override
    public List<String> getTaskIds(String filePath) throws IOException {
        List<String> taskIds = new ArrayList<>();
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            line = br.readLine(); // exclude header
            while ((line = br.readLine()) != null) {
                String[] lines = line.split(LINE_SEPARATOR);
                taskIds.add(lines[0]);
            }
        }
        return taskIds;
    }
}
