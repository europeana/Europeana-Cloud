package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraStaticContentDAO;
import eu.europeana.cloud.service.mcs.persistent.exception.ContentDaoNotFoundException;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ContentDAO;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author krystian.
 */
class DynamicContentProxyTest {

    @Test
    void shouldThrowExceptionOnNonExistingDAO() {
        //given
        final DynamicContentProxy instance = new DynamicContentProxy(prepareDAOMap(
                mock(S3ContentDAO.class)
        ));

        //then
        assertThrows(ContentDaoNotFoundException.class,
                () -> instance.deleteContent("exampleFileName", "exampleMd5", Storage.DATA_BASE));
    }

    @Test
    void shouldProperlySelectS3DeleteContent() throws FileNotExistsException {
        //given
        S3ContentDAO daoMock = mock(S3ContentDAO.class);
        final DynamicContentProxy instance = new DynamicContentProxy(prepareDAOMap(daoMock));

        //when
        instance.deleteContent("exampleFileName", "exampleMd5", Storage.OBJECT_STORAGE);

        //then
        verify(daoMock).deleteContent(anyString(), anyString());

  }

    @Test
    void shouldProperlySelectDataBaseDeleteContent() throws FileNotExistsException {
        //given
        CassandraContentDAO daoMock = mock(CassandraContentDAO.class);
        final DynamicContentProxy instance = new DynamicContentProxy(prepareDAOMap(daoMock));

        //when
        instance.deleteContent("exampleFileName", "exampleMd5", Storage.DATA_BASE);

        //then
        verify(daoMock).deleteContent(anyString(), anyString());

  }


    @Test
    void shouldProperlySelectDataBaseStaticDeleteContent() throws FileNotExistsException {
        //given
        CassandraStaticContentDAO daoMock = mock(CassandraStaticContentDAO.class);
        final DynamicContentProxy instance = new DynamicContentProxy(prepareDAOMap(daoMock));

        //when
        instance.deleteContent("exampleFileName", "exampleMd5", Storage.DB_STORAGE);

        //then
        verify(daoMock).deleteContent(anyString(), anyString());

  }

  private Map<Storage, ContentDAO> prepareDAOMap(final ContentDAO dao) {
    if (dao instanceof S3ContentDAO){
      return ImmutableMap.of(
          Storage.OBJECT_STORAGE, dao
      );
    } else if (dao instanceof CassandraContentDAO) {
      return ImmutableMap.of(
          Storage.DATA_BASE, dao
      );
    } else if (dao instanceof CassandraStaticContentDAO) {
      return ImmutableMap.of(
          Storage.DB_STORAGE, dao
      );
    }
    return null;
  }
}