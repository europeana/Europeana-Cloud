package eu.europeana.cloud.reader;

import eu.europeana.cloud.data.RevisionInformation;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by Tarek on 7/16/2019.
 */
public class CSVReaderTest {
    private CSVReader csvReader = new CSVReader();

    @Test
    public void shouldReadAndReturnTaskIdsForCSVFile() throws IOException {
        List<RevisionInformation> taskIds = csvReader.getRevisionsInformation(Paths.get("src/test/resources/revisions.csv").toString());
        assertNotNull(taskIds);
        assertEquals(8, taskIds.size());
    }
}