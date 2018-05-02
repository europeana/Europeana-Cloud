package eu.europeana.cloud.service.dps.storm.spouts.kafka.utils;

import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Created by Tarek on 5/2/2018.
 */
public class TaskSpoutInfo {
    private int fileCount;
    private boolean isStarted;
    private DpsTask dpsTask;

    public TaskSpoutInfo(DpsTask dpsTask) {
        this.fileCount = 0;
        isStarted = false;
        this.dpsTask = dpsTask;
    }

    public void inc() {
        fileCount++;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public void updateFileCount(int fileCount) {
        this.fileCount = this.fileCount + fileCount;

    }

    public void startTheTask() {
        isStarted = true;
    }

    public int getFileCount() {
        return fileCount;
    }

    public boolean isStarted() {
        return isStarted;
    }

    public DpsTask getDpsTask() {
        return dpsTask;
    }

}