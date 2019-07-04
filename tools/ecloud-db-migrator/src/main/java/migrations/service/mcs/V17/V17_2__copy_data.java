package migrations.service.mcs.V17;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.*;
import migrations.service.mcs.V17.jobs.DataCopier;

import java.util.*;
import java.util.concurrent.*;

import static migrations.common.TableCopier.hasNextRow;

/**
 * Created by pwozniak on 4/24/19
 */
public class V17_2__copy_data implements JavaMigration {

    private static final Log LOG = LogFactory.getLog(V17_2__copy_data.class);

    public static final int THREADS = 25;
    private PreparedStatement selectDistinctPartitionKeysStatement;


    private void initStatements(Session session) {
        selectDistinctPartitionKeysStatement = session.prepare("SELECT DISTINCT provider_id,dataset_id FROM latest_provider_dataset_representation_revision");
        selectDistinctPartitionKeysStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    @Override
    public void migrate(Session session) {
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS);

        try {
            initStatements(session);
            BoundStatement st = selectDistinctPartitionKeysStatement.bind();
            st.setFetchSize(10);
            ResultSet distinctPartitions = session.execute(st);
            Iterator<Row> distinctPartitionIterator = distinctPartitions.iterator();
            Set<Future<String>> set = new HashSet<>();
            List<KeyValue> keyValues = prepareKeyValues(distinctPartitionIterator);

            for (KeyValue kv : keyValues) {
                DataCopier dataCopier = new DataCopier(session, kv.provider_id, kv.dataset_id);
                LOG.info("Submitting task for: " + kv.provider_id + ":" + kv.dataset_id);
                executorService.submit(dataCopier);
            }
        } catch (Exception e) {
            LOG.error("The migration was not completed successfully:" + e.getMessage() + ". Because of:" + e.getCause()+". Please clean and restart again!!");
        }

        try {
            executorService.shutdown();
            executorService.awaitTermination(100, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private List<KeyValue> prepareKeyValues(Iterator<Row> distinctPartitionIterator){
        List<KeyValue> result = new ArrayList<>();
        int counter = 0;
        ///////////
        //Read from DB
        ///////////
//        while (hasNextRow(distinctPartitionIterator)) {
//
//            Row disRow = distinctPartitionIterator.next();
//            KeyValue kv = new KeyValue(disRow.getString("provider_id"), disRow.getString("dataset_id"));
//            result.add(kv);
//            System.out.print("\r"+counter);
//            counter++;
//        }
        /////////////////////////
        //Static list
        /////////////////////////
        result.add(new KeyValue("metis_production","0d624e40-853e-4b1c-8ffe-1906551ad7f5"));
        result.add(new KeyValue("metis_production","dc83993b-39fd-4f17-a65c-7e9947585519"));
        result.add(new KeyValue("metis_production","1349a88c-3218-4ba8-af98-505b1cc1ec78"));
        result.add(new KeyValue("metis_production","4e97157b-369f-4d04-a468-eb640f19a96d"));
        result.add(new KeyValue("metis_production","96238b57-6546-497e-bff9-c1ed6e8593cc"));
        result.add(new KeyValue("metis_production","be30c39b-bcea-433f-8839-160d8c19d746"));
        result.add(new KeyValue("metis_production","a78d2a44-f08a-421d-969d-811913d35138"));
        result.add(new KeyValue("metis_production","887655d4-8023-498e-8782-da45729c4d5b"));
        result.add(new KeyValue("metis_production","78f9883c-deac-4084-b1c5-9eb8319b2c85"));
        result.add(new KeyValue("metis_production","67dab0ca-49b3-4d0f-a6c6-8c6f244a39c6"));
        result.add(new KeyValue("metis_production","058cf6fe-f244-4c47-a44d-4abf940b8800"));
        result.add(new KeyValue("metis_production","f4dcf4b1-3274-4a9e-8d51-a6bb633e25fa"));
        result.add(new KeyValue("metis_production","3a890ca9-d4b2-4cea-8364-6c72e4b7e59d"));
        result.add(new KeyValue("metis_production","f5d9210a-776f-4760-860e-f70c34df8a7d"));
        result.add(new KeyValue("metis_production","d952303f-b813-4eaf-bf93-abbd1554a056"));
        result.add(new KeyValue("metis_production","feffe184-f578-4f31-b2b6-d4ee8cc4fef8"));

        return result;
    }
}

class KeyValue {
    public String provider_id;
    public String dataset_id;

    public KeyValue(String provider_id,String dataset_id){
        this.provider_id = provider_id;
        this.dataset_id = dataset_id;
    }
}
