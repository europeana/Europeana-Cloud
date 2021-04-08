package eu.europeana.cloud.persisted;

import eu.europeana.cloud.api.Remover;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.dps.storm.utils.*;
import org.apache.log4j.Logger;

/**
 * Created by Tarek on 4/16/2019.
 */
public class RemoverImpl implements Remover {

    static final Logger LOGGER = Logger.getLogger(RemoverImpl.class);

    private final CassandraSubTaskInfoDAO subTaskInfoDAO;
    private final CassandraTaskErrorsDAO taskErrorDAO;
    private final CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO;

    private static final int DEFAULT_RETRIES = 5;
    private static final int SLEEP_TIME = 3000;


    public RemoverImpl(String hosts, int port, String keyspaceName, String userName, String password) {
        CassandraConnectionProvider cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        subTaskInfoDAO = CassandraSubTaskInfoDAO.getInstance(cassandraConnectionProvider);
        taskErrorDAO = CassandraTaskErrorsDAO.getInstance(cassandraConnectionProvider);
        cassandraNodeStatisticsDAO = CassandraNodeStatisticsDAO.getInstance(cassandraConnectionProvider);
    }

    RemoverImpl(CassandraSubTaskInfoDAO subTaskInfoDAO, CassandraTaskErrorsDAO taskErrorDAO, CassandraNodeStatisticsDAO cassandraNodeStatisticsDAO) {
        this.subTaskInfoDAO = subTaskInfoDAO;
        this.taskErrorDAO = taskErrorDAO;
        this.cassandraNodeStatisticsDAO = cassandraNodeStatisticsDAO;
    }


    @Override
    public void removeNotifications(long taskId) {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                subTaskInfoDAO.removeNotifications(taskId);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while removing the logs. Retries left: " + retries);
                    waitForTheNextCall();
                } else {
                    LOGGER.error("Error while removing the logs.");
                    throw e;
                }
            }
        }
    }

    @Override
    public void removeErrorReports(long taskId) {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                taskErrorDAO.removeErrors(taskId);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while removing the error reports. Retries left: " + retries);
                    waitForTheNextCall();
                } else {
                    LOGGER.error("Error while removing the error reports.");
                    throw e;
                }
            }
        }

    }

    @Override
    public void removeStatistics(long taskId) {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                cassandraNodeStatisticsDAO.removeStatistics(taskId);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while removing the validation statistics. Retries left: " + retries);
                    waitForTheNextCall();
                } else {
                    LOGGER.error("rror while removing the validation statistics.");
                    throw e;
                }
            }
        }

    }

    private void waitForTheNextCall() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }


}
