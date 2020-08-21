package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.utils.GhostTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@Scope("request")
@RequestMapping("/diag")
public class DiagnosticResource {

    @Autowired
    private GhostTaskService ghostTaskService;

    @GetMapping("/ghostTasks")
    public List<TaskInfo> ghostTasks() {
        return ghostTaskService.findGhostTasks();
    }

}
