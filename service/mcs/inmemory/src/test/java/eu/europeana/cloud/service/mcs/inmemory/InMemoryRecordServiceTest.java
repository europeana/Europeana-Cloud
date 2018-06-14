package eu.europeana.cloud.service.mcs.inmemory;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.RepresentationSearchParams;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static junitparams.JUnitParamsRunner.$;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class InMemoryRecordServiceTest {

    private static final String SCHEMA = "PDF";
    private static final String PROVIDER_ID = "FBC";
    InMemoryRecordDAO recordDAO = mock(InMemoryRecordDAO.class);


    @Test
    @Parameters(method = "searchRepresentatonsParams")
    public void shouldSearchRepresentations(String threshold, int limit, int fromIndex, int toIndex, String nextSlice) {
        List<Representation> representations = createRepresentations(5, PROVIDER_ID, SCHEMA);
        when(recordDAO.findRepresentations(PROVIDER_ID, SCHEMA)).thenReturn(representations);
        InMemoryRecordService recordService = new InMemoryRecordService(recordDAO, null, null, null);
        RepresentationSearchParams searchParams = RepresentationSearchParams.builder().setDataProvider(PROVIDER_ID)
                .setSchema(SCHEMA).build();

        ResultSlice<Representation> actual = recordService.search(searchParams, threshold, limit);

        assertEquals("List of representations are not equal. ", representations.subList(fromIndex, toIndex),
            actual.getResults());
        assertEquals("Next slice ", actual.getNextSlice(), nextSlice);
    }


    private Object searchRepresentatonsParams() {
        return $($(null, -1, 0, 5, null), $(null, 0, 0, 5, null), $(null, -2, 0, 5, null), $(null, 1, 0, 1, "1"),
            $(null, 4, 0, 4, "4"), $(null, 5, 0, 5, null), $("0", 5, 0, 5, null), $("1", 5, 1, 5, null),
            $("1", 2, 1, 3, "3"));
    }


    private List<Representation> createRepresentations(int items, String dataProviderId, String schema) {
        List<Representation> representations = new ArrayList<>();
        for (int i = 0; i < items; i++) {
            representations.add(createRepresentation(i, dataProviderId, schema));
        }
        return representations;
    }


    private Representation createRepresentation(int index, String dataProviderId, String schema) {
        return new Representation(Integer.toString(index), schema, "1.0", null, null, dataProviderId,
                new ArrayList<File>(), false, new Date());
    }

}
