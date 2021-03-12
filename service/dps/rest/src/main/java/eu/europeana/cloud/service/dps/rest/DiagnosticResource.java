package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordDAO;
import eu.europeana.cloud.service.dps.utils.GhostTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@RestController
@Scope("request")
@RequestMapping("/diag")
public class DiagnosticResource {

    @Autowired
    private GhostTaskService ghostTaskService;

    @Autowired
    private HarvestedRecordDAO harvestedRecordDAO;

    @GetMapping("/ghostTasks")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public List<TaskInfo> ghostTasks() {
        return ghostTaskService.findGhostTasks();
    }


    @GetMapping("/harvestedRecords/{providerId}/{datasetId}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public List<HarvestedRecord> harvestedRecords(@PathVariable String providerId, @PathVariable String datasetId
            , @RequestParam(defaultValue = "10") int count, @RequestParam(required = false) String oaiId)    {
        if(oaiId!=null) {
            return Collections.singletonList(harvestedRecordDAO.findRecord(providerId, datasetId,oaiId).orElse(null));
        }else {
            List<HarvestedRecord> result = new ArrayList<>();

            Iterator<HarvestedRecord> it = harvestedRecordDAO.findDatasetRecords(providerId, datasetId);
            for (int i = 0; i < count && it.hasNext(); i++) {
                HarvestedRecord record = it.next();
                result.add(record);
            }
            return result;
        }
    }


}
