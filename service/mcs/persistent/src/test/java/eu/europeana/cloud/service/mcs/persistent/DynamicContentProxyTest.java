package eu.europeana.cloud.service.mcs.persistent;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraStaticContentDAO;
import eu.europeana.cloud.service.mcs.persistent.exception.ContentDaoNotFoundException;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ContentDAO;
import org.junit.Test;

import java.util.Map;

/**
 * @author krystian.
 */
public class DynamicContentProxyTest {

  @Test(expected = ContentDaoNotFoundException.class)
  public void shouldThrowExceptionOnNonExistingDAO() throws FileNotExistsException {
    //given
    final DynamicContentProxy instance = new DynamicContentProxy(prepareDAOMap(
                mock(S3ContentDAO.class)
    ));

    //then
    instance.deleteContent("exampleFileName", "exampleMd5", Storage.DATA_BASE);
  }

  @Test
  public void shouldProperlySelectS3DeleteContent() throws FileNotExistsException {
    //given
        S3ContentDAO daoMock = mock(S3ContentDAO.class);
    final DynamicContentProxy instance = new DynamicContentProxy(prepareDAOMap(daoMock));

    //when
    instance.deleteContent("exampleFileName","exampleMd5", Storage.OBJECT_STORAGE);

    //then
    verify(daoMock).deleteContent(anyString(), anyString());

  }

  @Test
  public void shouldProperlySelectDataBaseDeleteContent() throws FileNotExistsException {
    //given
    CassandraContentDAO daoMock = mock(CassandraContentDAO.class);
    final DynamicContentProxy instance = new DynamicContentProxy(prepareDAOMap(daoMock));

    //when
    instance.deleteContent("exampleFileName","exampleMd5", Storage.DATA_BASE);

    //then
    verify(daoMock).deleteContent(anyString(), anyString());

  }


  @Test
  public void shouldProperlySelectDataBaseStaticDeleteContent() throws FileNotExistsException {
    //given
    CassandraStaticContentDAO daoMock = mock(CassandraStaticContentDAO.class);
    final DynamicContentProxy instance = new DynamicContentProxy(prepareDAOMap(daoMock));

    //when
    instance.deleteContent("exampleFileName","exampleMd5", Storage.DATA_BASE_STATIC);

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
          Storage.DATA_BASE_STATIC, dao
      );
    }
    return null;
  }
}