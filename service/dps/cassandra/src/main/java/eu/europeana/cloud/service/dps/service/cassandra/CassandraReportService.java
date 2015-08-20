package eu.europeana.cloud.service.dps.service.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class CassandraReportService implements TaskExecutionReportService
{    
    CassandraConnectionProvider cassandra;

    public CassandraReportService(String hosts, int port, String keyspaceName, String userName, String password) 
    {
        cassandra = new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
    }    

    @Override
    public String getTaskProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException 
    {
        long taskId_ = Long.valueOf(taskId);
        
        JsonObject res = new JsonObject();
        res.addProperty("taskId", taskId_);

        //read basic informations from Cassandra
        Statement selectFromBasicInfo = QueryBuilder.select().all()
                .from(CassandraTablesAndColumnsNames.BASIC_INFO_TABLE)
                .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.BASIC_TASK_ID, taskId_));

        Row basicInfo = cassandra.getSession().execute(selectFromBasicInfo).one();

        //basicInfo == null means: task has been dropped or task is still running and calcutalion of expected size is in progress
        if(basicInfo != null)
        {
            int expectedSize = basicInfo.getInt(CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE);
            
            //read number of processed tasks from Cassandra
            Statement selectFromNotification = QueryBuilder.select().countAll()
                    .from(CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE)
                    .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID, taskId_))
                    .limit(expectedSize);
            
            ResultSet notifications = cassandra.getSession().execute(selectFromNotification);
            
            long processed = notifications.one().getLong("count");
            
            res.addProperty("topologyName", basicInfo.getString(CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME));
            res.addProperty("total_size", expectedSize);
            res.addProperty("processed", processed);           
        }
        else
        {
            //read number of processed tasks from Cassandra
            Statement selectFromNotification = QueryBuilder.select().countAll()
                    .from(CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE)
                    .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID, taskId_));
            
            ResultSet notifications = cassandra.getSession().execute(selectFromNotification);
            
            long processed = notifications.one().getLong("count");
            
            res.addProperty("topologyName", "");
            res.addProperty("total_size", "?");
            res.addProperty("processed", processed);            
        }

        return new Gson().toJson(res);
    }

    @Override
    public String getTaskNotification(String taskId) 
    {
        Statement selectFromNotification = QueryBuilder.select().all()
                .from(CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE)
                .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID, Long.valueOf(taskId)));
            
        ResultSet notifications = cassandra.getSession().execute(selectFromNotification);
        
        return new Gson().toJson(notificationResultsetToJsonObject(notifications));
    }

    @Override
    public void incrTaskProgress(String taskId) 
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    private JsonArray notificationResultsetToJsonObject(ResultSet data)
    {
        JsonArray res = new JsonArray();
        
        for(Row row: data)
        {
            JsonObject line = new JsonObject();
            
            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID, 
                    row.getLong(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID));
            
            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME, 
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME));
            
            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE, 
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE));
            
            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_STATE, 
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_STATE));
            
            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT, 
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT));
            
            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATIONS, 
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATIONS));
            
            res.add(line);
        }
        
        return res;
    }  
}
