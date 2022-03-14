package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.dps.services.MetisDatasetService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static eu.europeana.cloud.service.dps.RestInterfaceConstants.METIS_DATASET_PUBLISHED_RECORDS_SEARCH;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class MetisDatasetResourceTest {

    private final MetisDatasetService mock = Mockito.mock(MetisDatasetService.class);
    MockMvc mockMvc;

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.standaloneSetup(new MetisDatasetResource(mock))
                .build();
    }

    @Test
    public void shouldReturnBadRequestInCaseOfEmptyListOfRecords() throws Exception {

        mockMvc.perform(post(METIS_DATASET_PUBLISHED_RECORDS_SEARCH, "1")
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isBadRequest());
    }

}