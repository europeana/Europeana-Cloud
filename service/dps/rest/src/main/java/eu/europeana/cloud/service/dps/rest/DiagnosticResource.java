package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.services.postprocessors.PostProcessingService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.utils.GhostTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
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
    private HarvestedRecordsDAO harvestedRecordsDAO;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    @Autowired
    private PostProcessingService postProcessingService;

    @GetMapping("/ghostTasks")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public List<TaskInfo> ghostTasks() {
        return ghostTaskService.findGhostTasks();
    }


    @GetMapping("/harvestedRecords/{metisDatasetId}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public List<HarvestedRecord> harvestedRecords(@PathVariable String metisDatasetId
            , @RequestParam(defaultValue = "10") int count, @RequestParam(required = false) String oaiId)    {
        if(oaiId!=null) {
            return Collections.singletonList(harvestedRecordsDAO.findRecord(metisDatasetId,oaiId).orElse(null));
        } else {
            List<HarvestedRecord> result = new ArrayList<>();

            Iterator<HarvestedRecord> it = harvestedRecordsDAO.findDatasetRecords(metisDatasetId);
            for (var index = 0; index < count && it.hasNext(); index++) {
                HarvestedRecord theRecord = it.next();
                result.add(theRecord);
            }
            return result;
        }
    }


    @PostMapping("/postProcess/{taskId}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public void postProcess(long taskId) {
        taskInfoDAO.findById(taskId).ifPresent(taskInfo -> callPostProcess(taskInfo));
    }

    private void callPostProcess(TaskInfo taskInfo) {
        //tasksByStateDAO.findTask(taskInfo.getState().toString(), taskInfo.getTopologyName(), taskInfo.getId()).ifPresent(row -> );
    }

}
