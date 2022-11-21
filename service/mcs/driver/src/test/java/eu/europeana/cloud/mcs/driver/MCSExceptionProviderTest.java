package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class MCSExceptionProviderTest {

  @Test(expected = DriverException.class)
  public void shouldThrowDriverExceptionWhenNullErrorInfoPassed() {
    ErrorInfo errorInfo = null;
    MCSExceptionProvider.generateException(errorInfo);
  }


  @Test(expected = DriverException.class)
  public void shouldThrowDriverExceptionWhenUnknownErrorInfoCodePassed() {
    ErrorInfo errorInfo = new ErrorInfo("THIS_IS_REALLY_WRONG_CODE", null);
    MCSExceptionProvider.generateException(errorInfo);
  }


  @Test
  @Parameters(method = "statusCodes")
  public void shouldReturnCorrectException(Throwable ex, String errorCode) {
    ErrorInfo errorInfo = new ErrorInfo(errorCode, "details");
    MCSException exception = MCSExceptionProvider.generateException(errorInfo);
    Assert.assertEquals(ex.getClass(), exception.getClass());
    assertThat(exception.getMessage(), is(errorInfo.getDetails()));
  }


  /*
      This one is separate because it returns different details message...
  */
  @Test
  public void shouldReturnRecordNotExistsException() {
    ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.RECORD_NOT_EXISTS.toString(), "details");
    MCSException exception = MCSExceptionProvider.generateException(errorInfo);
    assertTrue(exception instanceof RecordNotExistsException);
    assertThat(exception.getMessage(), is("There is no record with provided global id: " + errorInfo.getDetails()));
  }


  @Test(expected = DriverException.class)
  public void shouldThrowDriverException() {
    ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.toString(), "details");
    MCSExceptionProvider.generateException(errorInfo);
  }

  @SuppressWarnings("unused")
  private Object[] statusCodes() {
    return new Object[]{
        new Object[]{new CannotModifyPersistentRepresentationException(),
            McsErrorCode.CANNOT_MODIFY_PERSISTENT_REPRESENTATION.toString()},
        new Object[]{new DataSetAlreadyExistsException(), McsErrorCode.DATASET_ALREADY_EXISTS.toString()},
        new Object[]{new DataSetNotExistsException(), McsErrorCode.DATASET_NOT_EXISTS.toString()},
        new Object[]{new FileAlreadyExistsException(), McsErrorCode.FILE_ALREADY_EXISTS.toString()},
        new Object[]{new FileNotExistsException(), McsErrorCode.FILE_NOT_EXISTS.toString()},
        new Object[]{new ProviderNotExistsException(), McsErrorCode.PROVIDER_NOT_EXISTS.toString()},
        new Object[]{new RepresentationNotExistsException(), McsErrorCode.REPRESENTATION_NOT_EXISTS.toString()},
        new Object[]{new FileContentHashMismatchException(), McsErrorCode.FILE_CONTENT_HASH_MISMATCH.toString()},
        new Object[]{new RepresentationAlreadyInSet(), McsErrorCode.REPRESENTATION_ALREADY_IN_SET.toString()},
        new Object[]{new CannotPersistEmptyRepresentationException(),
            McsErrorCode.CANNOT_PERSIST_EMPTY_REPRESENTATION.toString()},
        new Object[]{new WrongContentRangeException(), McsErrorCode.WRONG_CONTENT_RANGE.toString()}
    };
  }

}
