package eu.europeana.cloud.service.uis.dao;

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.uis.TestAAConfiguration;
import eu.europeana.cloud.service.uis.service.CassandraTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {TestAAConfiguration.class})
public class CloudIdDAOTest extends CassandraTestBase {

    @Autowired
    private LocalIdDAO localIdDao;

    @Autowired
    private CloudIdDAO service;

    @Autowired
    private CassandraDataProviderDAO dataProviderDao;

    @Test
    public void deleteCloudIdShouldRemoveCloudId() throws Exception {
        // given
        final String providerId = "providerId";
        final String recordId = "recordId";
        final String id = "id";
        dataProviderDao.createDataProvider(providerId, new DataProviderProperties());
        localIdDao.insert(id, providerId, recordId);

        assertTrue(service.insert(id, providerId, recordId).isPresent());
        assertEquals(1, service.searchById(id).size());
        service.delete(id, providerId, recordId);
        assertEquals(0, service.searchById(id).size());
    }
}
