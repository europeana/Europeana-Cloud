package eu.europeana.cloud.service.dps.rest.exceptionmappers;

import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;

import javax.ws.rs.ext.ExceptionMapper;

public class TaskSubmissionExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<TaskSubmissionException> {
}
