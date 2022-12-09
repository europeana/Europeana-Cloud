package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.TASK_ID;
import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.createAndStoreErrorType;
import static org.junit.Assert.assertEquals;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils;
import eu.europeana.cloud.test.CassandraTestInstance;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class CassandraTaskErrorsDAOTest extends CassandraTestBase {

  public static final String ERROR_TYPE_1 = "03e473e0-a201-11e7-a8ab-0242ac110009";
  public static final String ERROR_TYPE_2 = "03e473e0-a201-11e7-a8ab-0242ac110010";
  private static final String ERROR_TYPE_3 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b3";
  private static final String ERROR_TYPE_4 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b4";
  private static final String ERROR_TYPE_5 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b5";
  private static final String RESOURCE_1 = "some_resource_1";
  private static final String RESOURCE_2 = "some_resource_2";
  private static final String RESOURCE_3 = "some_resource_3";
  private static final String RESOURCE_4 = "some_resource_4";
  private static final String RESOURCE_5 = "some_resource_5";
  private CassandraTaskErrorsDAO cassandraTaskErrorsDAO;

  @Before
  public void setup() {
    CassandraConnectionProvider db = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST,
        CassandraTestInstance.getPort(), KEYSPACE, USER,
        PASSWORD);
    cassandraTaskErrorsDAO = CassandraTaskErrorsDAO.getInstance(db);
  }

  @Test
  public void shouldReturnZeroErrorsForTaskThatDoesNotExist() {
    long errorCount = cassandraTaskErrorsDAO.selectErrorCountsForErrorType(1, UUID.fromString(ERROR_TYPE_1));
    assertEquals(0, errorCount);
  }

  @Test
  public void shouldReturnAllErrorTypes() {
    createAndStoreErrorType(ERROR_TYPE_1, cassandraTaskErrorsDAO);
    createAndStoreErrorType(ERROR_TYPE_2, cassandraTaskErrorsDAO);
    createAndStoreErrorType(ERROR_TYPE_3, cassandraTaskErrorsDAO);
    createAndStoreErrorType(ERROR_TYPE_4, cassandraTaskErrorsDAO);
    createAndStoreErrorType(ERROR_TYPE_5, cassandraTaskErrorsDAO);
    assertEquals(5, cassandraTaskErrorsDAO.getErrorTypes(TASK_ID).size());
  }

  @Test
  public void shouldReturnEmptyListWhenNoErrorTypeRecords() {
    assertEquals(0, cassandraTaskErrorsDAO.getErrorTypes(TASK_ID).size());
  }

  @Test
  public void shouldReturnEmptyListWhenNoErrorNotificationsRecords() {
    assertEquals(0,
        cassandraTaskErrorsDAO.getErrorNotificationsWithGivenLimit(TASK_ID, UUID.fromString(ERROR_TYPE_1), 10).size());
  }

  @Test
  public void shouldReturnErrorNotificationsAccordinglyToLimit() {
    ServiceAndDAOTestUtils.createAndStoreErrorNotification(ERROR_TYPE_1, cassandraTaskErrorsDAO, RESOURCE_1);
    ServiceAndDAOTestUtils.createAndStoreErrorNotification(ERROR_TYPE_1, cassandraTaskErrorsDAO, RESOURCE_2);
    ServiceAndDAOTestUtils.createAndStoreErrorNotification(ERROR_TYPE_1, cassandraTaskErrorsDAO, RESOURCE_3);
    ServiceAndDAOTestUtils.createAndStoreErrorNotification(ERROR_TYPE_1, cassandraTaskErrorsDAO, RESOURCE_4);
    ServiceAndDAOTestUtils.createAndStoreErrorNotification(ERROR_TYPE_1, cassandraTaskErrorsDAO, RESOURCE_5);
    assertEquals(1, cassandraTaskErrorsDAO.getErrorNotificationsWithGivenLimit(TASK_ID, UUID.fromString(ERROR_TYPE_1), 1).size());
    assertEquals(3, cassandraTaskErrorsDAO.getErrorNotificationsWithGivenLimit(TASK_ID, UUID.fromString(ERROR_TYPE_1), 3).size());
    assertEquals(5, cassandraTaskErrorsDAO.getErrorNotificationsWithGivenLimit(TASK_ID, UUID.fromString(ERROR_TYPE_1), 5).size());
    assertEquals(5, cassandraTaskErrorsDAO.getErrorNotificationsWithGivenLimit(TASK_ID, UUID.fromString(ERROR_TYPE_1), 6).size());
  }

  @Test
  public void shouldReturnCorrectNumberOfErrorsForTask() {
    cassandraTaskErrorsDAO.insertErrorCounter(1, ERROR_TYPE_1, 4);
    cassandraTaskErrorsDAO.insertErrorCounter(1, ERROR_TYPE_2, 1);

    long errorCount = cassandraTaskErrorsDAO.selectErrorCountsForErrorType(1, UUID.fromString(ERROR_TYPE_1));
    assertEquals(4, errorCount);

    errorCount = cassandraTaskErrorsDAO.selectErrorCountsForErrorType(1, UUID.fromString(ERROR_TYPE_2));
    assertEquals(1, errorCount);
  }


}