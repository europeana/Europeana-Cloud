package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.exception.ContentDaoNotFoundException;
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author krystian.
 */
public class DynamicContentDAOTest {

    @Test(expected = ContentDaoNotFoundException.class)
    public void shouldThrowExceptionOnNonExistingDAO() throws FileNotExistsException {
        //given
        final DynamicContentDAO instance = new DynamicContentDAO(prepareDAOMap(
                mock(SwiftContentDAO.class)
        ));

        //then
        instance.deleteContent("exampleFileName",Storage.DATA_BASE);
    }

    @Test
    public void shouldProperlySelectDataBaseDeleteContent() throws FileNotExistsException {
        //given
        SwiftContentDAO daoMock = mock(SwiftContentDAO.class);
        final DynamicContentDAO instance = new DynamicContentDAO(prepareDAOMap(daoMock));

        //when
        instance.deleteContent("exampleFileName",Storage.OBJECT_STORAGE);

        //then
        verify(daoMock).deleteContent(anyString());

    }

    private Map<Storage, ContentDAO> prepareDAOMap(final ContentDAO dao) {
        return ImmutableMap.of(
                Storage.OBJECT_STORAGE, dao
        );
    }
}