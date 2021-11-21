package eu.europeana.cloud.service.uis.dao;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.Lists;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketSize;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.service.CassandraUniqueIdentifierService;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Dao providing access to the search based on record id and provider id
 * operations
 *
 * @author Yorgos.Mamakis@ kb.nl
 */
public class CassandraLocalIdDAO {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraLocalIdDAO.class);
    private static final String PROVIDER_RECORD_ID_BUCKETS_TABLE = "provider_record_id_buckets";
    private final CassandraConnectionProvider dbService;
    private PreparedStatement insertStatement;
    private PreparedStatement deleteStatement;
    private PreparedStatement searchByProviderStatement;
    private PreparedStatement searchByRecordIdStatement;
    private PreparedStatement searchByProviderPaginatedStatement;

    @Autowired
    private BucketsHandler bucketsHandler;

    /**
     * The LocalId Dao
     *
     * @param dbService The service that exposes the database connection
     */
    public CassandraLocalIdDAO(CassandraConnectionProvider dbService) {
        this.dbService = dbService;
        prepareStatements();
    }

    private void prepareStatements() {
        insertStatement = dbService.getSession().prepare(
                "INSERT INTO Provider_Record_Id(provider_id,bucket_id, record_id,cloud_id) VALUES(?,?,?,?)");
        insertStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        deleteStatement = dbService.getSession().prepare(
                "DELETE FROM Provider_Record_Id WHERE provider_id= ? AND bucket_id = ? AND record_Id= ?");
        deleteStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        searchByProviderStatement = dbService.getSession().prepare(
                "SELECT * FROM Provider_Record_Id WHERE provider_id = ? AND bucket_id = ?");
        searchByProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        searchByRecordIdStatement = dbService.getSession().prepare(
                "SELECT * FROM Provider_Record_Id WHERE provider_id=? AND bucket_id = ? AND record_id=?");
        searchByRecordIdStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        searchByProviderPaginatedStatement = dbService.getSession().prepare(
                "SELECT * FROM Provider_Record_Id WHERE provider_id=? AND bucket_id = ? AND record_id>=? LIMIT ?");
        searchByProviderPaginatedStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    @Retryable
    public List<CloudId> searchById(String... args) throws DatabaseConnectionException {
        try {
            ResultSet rs = null;
            List<CloudId> result = new ArrayList<>();
            int bucketTreversedCount=0;
            Bucket bucket = bucketsHandler.getPreviousBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE, args[0]);
            while (bucket != null) {
                if (args.length == 1) {
                    rs = dbService.getSession().execute(searchByProviderStatement.bind(args[0], UUID.fromString(bucket.getBucketId())));
                } else if (args.length >= 2) {
                    rs = dbService.getSession().execute(searchByRecordIdStatement.bind(args[0], UUID.fromString(bucket.getBucketId()), args[1]));
                }
                bucketTreversedCount++;
                result.addAll(createCloudIdsFromRs(rs));
                if (result.size() > 0) {
                    break;
                }
                bucket = bucketsHandler.getPreviousBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE, args[0], bucket);
            }
            LOGGER.info("Seaching by localId result size: {}, performed {} record searches on different buckets."
                    ,result.size(),bucketTreversedCount);
            return result;
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(dbService.getHosts(), dbService.getPort(), e.getMessage())));
        }
    }

    @Retryable
    public List<Boolean> localIdsExists(String provider, List<CloudId> localIds) throws DatabaseConnectionException {
        try {
            List<Boolean> result = new ArrayList<>();
            localIds.stream().forEach(id->result.add(false));

            int bucketTreversedCount=0;
            int recordSearchCount=0;
            List<Bucket> list = bucketsHandler.getAllBuckets(PROVIDER_RECORD_ID_BUCKETS_TABLE, provider);
            list=Lists.reverse(list);

            //Bucket bucket = bucketsHandler.getPreviousBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE,provider);
            for(Bucket bucket:list) {
                for(int i=0;i<localIds.size();i++) {
                    if(!result.get(i)) {
                        String recordId = localIds.get(i).getLocalId().getRecordId();
                        ResultSet rs = dbService.getSession().execute(searchByRecordIdStatement.bind(provider, UUID.fromString(bucket.getBucketId()), recordId));
                        recordSearchCount++;
                        if (!createCloudIdsFromRs(rs).isEmpty()) {
                            LOGGER.info("Found record id in bucket no {}", bucketTreversedCount);
                            result.set(i, true);
                        }
                    }
                }
                bucketTreversedCount++;
                if (result.stream().allMatch(e -> e)) {
                    break;
                }
              //  bucket = bucketsHandler.getPreviousBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE, provider, bucket);
            }

            LOGGER.info("Checking if localids exists result: {}, traversed {} buckets and performed {} record searches."
                    ,result,bucketTreversedCount,recordSearchCount);
            return result;
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(dbService.getHosts(), dbService.getPort(), e.getMessage())));
        }
    }

    /**
     * Enable pagination search on active local Id information
     *
     * @param start      Record to start from
     * @param end        The number of record to retrieve
     * @param providerId The provider Identifier
     * @return A list of CloudId objects
     */
    @Retryable
    public List<CloudId> searchByIdWithPagination(String start, int end, String providerId) {
        List<CloudId> result = new ArrayList<>();

        Bucket bucket = bucketsHandler.getNextBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE, providerId);
        while (bucket != null) {
            ResultSet rs = dbService.getSession().execute(searchByProviderPaginatedStatement.bind(
                    providerId, UUID.fromString(bucket.getBucketId()), start, end));

            result.addAll(createCloudIdsFromRs(rs));
            if (result.size() >= end) {
                break;
            }
            bucket = bucketsHandler.getNextBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE, providerId, bucket);
        }
        return result;
    }

    public List<CloudId> insert(String... args) throws DatabaseConnectionException {
        try {
            Bucket bucket = bucketsHandler.getCurrentBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE, args[0]);
            if (bucket == null || bucket.getRowsCount() >= BucketSize.PROVIDER_RECORD_ID_TABLE) {
                bucket = new Bucket(args[0], new com.eaio.uuid.UUID().toString(), 0);
            }
            bucketsHandler.increaseBucketCount(PROVIDER_RECORD_ID_BUCKETS_TABLE, bucket);

            dbService.getSession().execute(insertStatement.bind(args[0], UUID.fromString(bucket.getBucketId()), args[1], args[2]));
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(
                            dbService.getHosts(), dbService.getPort(), e.getMessage())
            ));
        }
        List<CloudId> cIds = new ArrayList<>();
        CloudId cId = new CloudId();
        LocalId lId = new LocalId();
        lId.setProviderId(args[0]);
        lId.setRecordId(args[1]);
        cId.setLocalId(lId);
        cId.setId(args[2]);
        cIds.add(cId);
        return cIds;
    }

    public void delete(String providerId, String recordId) throws DatabaseConnectionException {
        try {
            Bucket bucket = bucketsHandler.getNextBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE, providerId);
            while (bucket != null) {

                ResultSet rs = dbService.getSession().execute(searchByRecordIdStatement.bind(
                        providerId, UUID.fromString(bucket.getBucketId()), recordId));

                if (rs.getAvailableWithoutFetching() == 1) {
                    dbService.getSession().execute(deleteStatement.bind(
                            providerId, UUID.fromString(bucket.getBucketId()), recordId));

                    if (bucket.getRowsCount() == 1) {
                        bucketsHandler.removeBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE, bucket);
                    } else {
                        bucketsHandler.decreaseBucketCount(PROVIDER_RECORD_ID_BUCKETS_TABLE, bucket);
                    }
                    break;
                }
                bucket = bucketsHandler.getNextBucket(PROVIDER_RECORD_ID_BUCKETS_TABLE, providerId, bucket);
            }
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(
                            dbService.getHosts(), dbService.getPort(), e.getMessage())
            ));
        }
    }

    private List<CloudId> createCloudIdsFromRs(ResultSet rs) {
        List<CloudId> cloudIds = new ArrayList<>();
        if (rs != null) {
            for (Row row : rs.all()) {
                LocalId lId = new LocalId();
                lId.setProviderId(row.getString("provider_Id"));
                lId.setRecordId(row.getString("record_Id"));
                CloudId cloudId = new CloudId();
                cloudId.setId(row.getString("cloud_id"));
                cloudId.setLocalId(lId);
                cloudIds.add(cloudId);
            }
        }

        return cloudIds;
    }
}
