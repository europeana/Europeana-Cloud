package eu.europeana.cloud.reader;

import eu.europeana.cloud.api.RevisionsReader;
import eu.europeana.cloud.data.RevisionInformation;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tarek on 7/15/2019.
 */
public class CSVReader implements RevisionsReader {

  private static final String LINE_SEPARATOR = ",";

  @Override
  public List<RevisionInformation> getRevisionsInformation(String filePath) throws IOException {
    List<RevisionInformation> revisionInformationList = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
      String line = "";
      br.readLine(); // exclude header
      while ((line = br.readLine()) != null) {
        String[] lines = line.split(LINE_SEPARATOR);
        RevisionInformation revisionInformation = new RevisionInformation(lines[0], lines[1], lines[2], lines[3], lines[4],
            lines[5]);
        revisionInformationList.add(revisionInformation);
      }
    }
    return revisionInformationList;
  }
}

