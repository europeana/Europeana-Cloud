package eu.europeana.cloud.service.uis.dao;

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.uis.TestAAConfiguration;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.service.CassandraTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {TestAAConfiguration.class})
public class CassandraCloudIdDAOTest extends CassandraTestBase {

    @Autowired
    private CassandraLocalIdDAO localIdDao;

    @Autowired
    private CassandraCloudIdDAO service;

    @Autowired
    private CassandraDataProviderDAO dataProviderDao;

    @Test(expected = CloudIdAlreadyExistException.class)
    public void insert_tryInsertTheSameContentTwice_ThrowsCloudIdAlreadyExistException()
            throws Exception {
        // given
        final String providerId = "providerId";
        final String recordId = "recordId";
        final String id = "id";
        service.insert(true, id, providerId, recordId);
        // when
        service.insert(true, id, providerId, recordId);
        // then
        assertEquals(1, service.searchById(id).size());
    }

    @Test(expected = CloudIdDoesNotExistException.class)
    public void deleteCloudIdShouldRemoveCloudId() throws Exception {
        // given
        final String providerId = "providerId";
        final String recordId = "recordId";
        final String id = "id";
        dataProviderDao.createDataProvider(providerId,
                new DataProviderProperties());
        localIdDao.insert(id, providerId, recordId);

        final int size = service.insert(false, id, providerId, recordId).size();
        assertEquals(1, size);
        assertEquals(1, service.searchById(id).size());
        service.delete(id, providerId, recordId);
        assertEquals(0, service.searchById(id).size());
    }


}
