package eu.europeana.cloud.service.mcs.inmemory;

import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import org.mockito.Mockito;

@RunWith(JUnitParamsRunner.class)
public class InMemoryDataSetServiceTest {

    private InMemoryDataSetDAO datasetDao;
    private String providerId = "FBC";
    private String dataSetId = "Books";
    private List<Representation> representations;
    private InMemoryDataSetService dataSetService;
    private UISClientHandler uisHandler;


    @Before
    public void setUp()
            throws Exception {
        datasetDao = mock(InMemoryDataSetDAO.class);
        uisHandler = mock(UISClientHandler.class);
        Mockito.doReturn(true).when(uisHandler).providerExistsInUIS(Mockito.anyString());

        representations = new ArrayList<>();
        representations = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            representations.add(createRepresentation(i));
        }
        when(datasetDao.listDataSet(providerId, dataSetId)).thenReturn(representations);
        InMemoryRecordDAO recordDao = new InMemoryRecordListDAO(representations);
        dataSetService = new InMemoryDataSetService(datasetDao, recordDao, uisHandler);
    }


    @Test
    @Parameters(method = "listDatasetParams")
    public void shouldListDataSet(String threshold, int limit, String nextSlice, int fromIndex, int toIndex)
            throws Exception {
        ResultSlice<Representation> actual = dataSetService.listDataSet(providerId, dataSetId, threshold, limit);

        assertThat("Next slice should be equal '" + nextSlice + "' but was '" + actual.getNextSlice() + "'",
            actual.getNextSlice(), equalTo(nextSlice));
        assertThat("Lists of representations are not equal", actual.getResults(),
            equalTo(representations.subList(fromIndex, toIndex)));
    }


    @Test
    public void shouldListEmptyDataSet()
            throws Exception {
        when(datasetDao.listDataSet(providerId, dataSetId)).thenReturn(new ArrayList<Representation>());
        InMemoryRecordDAO recordDao = new InMemoryRecordListDAO(new ArrayList<Representation>());
        dataSetService = new InMemoryDataSetService(datasetDao, recordDao, uisHandler);

        ResultSlice<Representation> actual = dataSetService.listDataSet(providerId, dataSetId, null, 100);

        assertThat("Next slice should be null, but was '" + actual.getNextSlice() + "'", actual.getNextSlice(),
            nullValue());
        assertTrue("List of representations should be empty, but was: " + actual.getResults(), actual.getResults()
                .isEmpty());
    }


    private Object listDatasetParams() {
        return $($(null, 0, null, 0, 5), $(null, 5, null, 0, 5), $(null, 3, "3", 0, 3), $("3", 2, null, 3, 5),
            $(null, 2, "2", 0, 2), $("2", 2, "4", 2, 4));
    }


    private final static class InMemoryRecordListDAO extends InMemoryRecordDAO {

        private final List<Representation> representations;


        InMemoryRecordListDAO(List<Representation> representations) {
            super();
            this.representations = Collections.unmodifiableList(representations);
        }


        @Override
        public Representation getRepresentation(String globalId, String schema, String version)
                throws RepresentationNotExistsException, VersionNotExistsException {
            for (Representation representation : representations) {
                if (representation.getCloudId().equals(globalId)
                        && representation.getRepresentationName().equals(schema)
                        && representation.getVersion().equals(version)) {
                    return representation;
                }
            }
            throw new RepresentationNotExistsException(String.format(
                "Representation (id=%s, schema=%s, version=%s) does not exist.", globalId, schema, version));
        }
    }


    private Representation createRepresentation(int index) {
        return new Representation(Integer.toString(index), "PDF", "1.0", null, null, "FBC", new ArrayList<File>(),
                false, new Date());
    }

}
