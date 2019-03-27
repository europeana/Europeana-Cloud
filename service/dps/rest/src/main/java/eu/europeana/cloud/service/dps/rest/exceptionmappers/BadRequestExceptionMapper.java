package eu.europeana.cloud.service.dps.rest.exceptionmappers;



import javax.validation.ConstraintViolationException;
import javax.ws.rs.ext.ExceptionMapper;

/**
 * Created by Tarek on 2/15/2019.
 */
public class BadRequestExceptionMapper  extends UnitedExceptionMapper implements
        ExceptionMapper<ConstraintViolationException> {
}
