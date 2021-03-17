package eu.europeana.cloud.service.dps.storm.utils;

import com.google.common.collect.Streams;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord.builder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HarvestedRecordDAOTest extends CassandraTestBase{

    private static final String DATASET_ID = "testDatasetId";
    private static final String PROVIDER_ID = "testProviderId";
    private static final String OAI_ID_1 = "http://data.europeana.eu/item/2058621/LoCloud_census_1891_00037ace_1df9_438f_96eb_ea37bc646ec9";
    private static final String OAI_ID_2 = "http://data.europeana.eu/item/2058621/object_NRA_9857684";
    private static final String OAI_ID_3 = "http://data.europeana.eu/item/2058621/LoCloud_census_1891_008283c8_8ed6_49ab_9082_44ab4317d62a";
    private static final Date HARVESTED_DATE = new Date();
    private HarvestedRecordDAO dao;

    @Before
    public void setup(){
        CassandraConnectionProvider db = new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER, PASSWORD);
        dao = new HarvestedRecordDAO(db);
    }

    @Test
    public void shouldFindInsertedRecords(){
        dao.insertHarvestedRecord(builder().providerId(PROVIDER_ID).datasetId(DATASET_ID).oaiId(OAI_ID_1).harvestDate(HARVESTED_DATE).build());
        dao.insertHarvestedRecord(builder().providerId(PROVIDER_ID).datasetId(DATASET_ID).oaiId(OAI_ID_2).harvestDate(HARVESTED_DATE).build());

        Optional<HarvestedRecord> record = dao.findRecord(PROVIDER_ID, DATASET_ID, OAI_ID_1);

        assertEquals(PROVIDER_ID,record.get().getProviderId());
        assertEquals(DATASET_ID,record.get().getDatasetId());
        assertEquals(OAI_ID_1,record.get().getOaiId());
        assertEquals(HARVESTED_DATE, record.get().getHarvestDate());
    }

    @Test
    public void shouldReturnEmptyOptionalWhenNoRecordFound(){

        Optional<HarvestedRecord> record = dao.findRecord(PROVIDER_ID, DATASET_ID, OAI_ID_3);

        assertTrue(record.isEmpty());
    }

    @Test
    public void shouldIterateThrowDatasetRecrods(){
        dao.insertHarvestedRecord(builder().providerId(PROVIDER_ID).datasetId(DATASET_ID).oaiId(OAI_ID_1).harvestDate(HARVESTED_DATE).build());
        dao.insertHarvestedRecord(builder().providerId(PROVIDER_ID).datasetId(DATASET_ID).oaiId(OAI_ID_2).harvestDate(HARVESTED_DATE).build());

        Iterator<HarvestedRecord> iterator = dao.findDatasetRecords(PROVIDER_ID, DATASET_ID);

        List<HarvestedRecord> result = Streams.stream(iterator).collect(Collectors.toList());

        assertEquals(2,result.size());
        assertEquals(1,result.stream().filter(record -> record.getOaiId().equals(OAI_ID_1)).count());
        assertEquals(1,result.stream().filter(record -> record.getOaiId().equals(OAI_ID_2)).count());
    }

}