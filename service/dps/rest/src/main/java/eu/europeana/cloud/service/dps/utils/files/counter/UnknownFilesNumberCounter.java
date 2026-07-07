package eu.europeana.cloud.service.dps.utils.files.counter;

import static eu.europeana.cloud.common.model.dps.TaskInfo.UNKNOWN_EXPECTED_RECORDS_NUMBER;

import eu.europeana.cloud.service.dps.DpsTask;

public class UnknownFilesNumberCounter extends FilesCounter {

  @Override
  public int getFilesCount(DpsTask task) {
    return UNKNOWN_EXPECTED_RECORDS_NUMBER;
  }
}
