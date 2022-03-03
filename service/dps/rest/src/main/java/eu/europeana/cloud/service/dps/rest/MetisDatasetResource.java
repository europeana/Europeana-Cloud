package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.MetisDataset;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.services.MetisDatasetService;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import static eu.europeana.cloud.service.dps.RestInterfaceConstants.METIS_DATASETS;

@RestController
@RequestMapping(METIS_DATASETS)
public class MetisDatasetResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetisDatasetResource.class);

    private final MetisDatasetService metisDatasetService;

    public MetisDatasetResource(MetisDatasetService metisDatasetService){
        this.metisDatasetService = metisDatasetService;
    }
    @GetMapping(produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    public MetisDataset getMetisDatasetStats(@PathVariable String datasetId,
                                             @RequestParam(value = "database") TargetIndexingDatabase targetIndexingDatabase) throws IndexingException {
        LOGGER.info("Reading dataset stats for datasetId: {}", datasetId);
        MetisDataset metisDataset = MetisDataset.builder()
                .id(datasetId)
                .build();

        return metisDatasetService.prepareStatsFor(metisDataset, targetIndexingDatabase/*, targetIndexingEnvironment*/);
    }
}
