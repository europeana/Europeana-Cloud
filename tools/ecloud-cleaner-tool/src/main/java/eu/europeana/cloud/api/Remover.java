package eu.europeana.cloud.api;

/**
 * Created by Tarek on 4/16/2019.
 */
public interface Remover {

    void removeNotifications(long taskId);

    void removeErrorReports(long taskId);

    void removeStatistics(long taskId);
}
