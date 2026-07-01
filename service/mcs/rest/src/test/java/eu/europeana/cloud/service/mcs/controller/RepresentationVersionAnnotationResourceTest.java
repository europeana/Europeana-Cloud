package eu.europeana.cloud.service.mcs.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.common.model.RepresentationVersionAnnotation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.dto.AnnotationsDto;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;

import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.getBaseUri;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.any;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class RepresentationVersionAnnotationResourceTest extends AbstractResourceTest {

  @Test
  void shouldNotAllowToAddAnnotationForUserWithoutPermissions()
          throws Exception {

    DataSetPermissionsVerifier dataSetPermissionsVerifier = applicationContext.getBean(DataSetPermissionsVerifier.class);
    Mockito.doReturn(false).when(dataSetPermissionsVerifier).isUserAllowedToAddAnnotationTo(Mockito.any());

    ObjectMapper mapper = new ObjectMapper();
    AnnotationsDto annotationsDto = new AnnotationsDto();

    annotationsDto.setAnnotation(new RepresentationVersionAnnotation(RepresentationVersionAnnotation.AnnotationKey.INVALID,""));

    mockMvc.perform(post(URITools.getRepresentationVersionAnnotationUri(getBaseUri(),"cloudId","schema","version"))
                    .contentType(MediaType.APPLICATION_JSON_VALUE)
                    .content(mapper.writeValueAsString(annotationsDto)))
            .andExpect(status().isMethodNotAllowed());
  }

  @Test
  void shouldAllowToAddAnnotationForUserWithoutPermissions() throws Exception {

    DataSetPermissionsVerifier dataSetPermissionsVerifier =
            applicationContext.getBean(DataSetPermissionsVerifier.class);
    Mockito.doReturn(true).when(dataSetPermissionsVerifier).isUserAllowedToAddAnnotationTo(Mockito.any());
    RecordService recordService = applicationContext.getBean(RecordService.class);

    ObjectMapper mapper = new ObjectMapper();
    AnnotationsDto annotationsDto = new AnnotationsDto();

    annotationsDto.setAnnotation(new RepresentationVersionAnnotation(RepresentationVersionAnnotation.AnnotationKey.INVALID,""));

    mockMvc.perform(
            post(
                    URITools.getRepresentationVersionAnnotationUri(getBaseUri(), "cloudId", "schema", "version"))
                    .contentType(MediaType.APPLICATION_JSON_VALUE)
                    .content(mapper.writeValueAsString(annotationsDto)))
            .andExpect(status().isOk());

    verify(recordService, times(1)).addAnnotationToRepresentationVersion(any(), any());
  }

}