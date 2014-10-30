package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.common.web.AASParamConstants.Q_USER_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_VER;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Date;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import junitparams.JUnitParamsRunner;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.CannotModifyPersistentRepresentationExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.CannotPersistEmptyRepresentationExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RecordNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RepresentationNotExistsExceptionMapper;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.VersionNotExistsExceptionMapper;

@RunWith(JUnitParamsRunner.class)
public class RepresentationVersionAuthorizationResourceTest extends JerseyTest {

    private MutableAclService mutableAclService;

    static final private String globalId = "1";
    static final private String schema = "DC";
    static final private String version = "1.0";
    static final private String fileName = "1.xml";
    static final private String persistPath = URITools.getPath(RepresentationVersionResource.class,
        "persistRepresentation", globalId, schema, version).toString();
    static final private String copyPath = URITools.getPath(RepresentationVersionResource.class, "copyRepresentation",
        globalId, schema, version).toString();

    static final private Representation representation = new Representation(globalId, schema, version, null, null,
            "DLF", Arrays.asList(new File(fileName, "text/xml", "91162629d258a876ee994e9233b2ad87", "2013-01-01",
                    12345, null)), true, new Date());

    @Override
    public Application configure() {
        return new ResourceConfig().registerClasses(RepresentationVersionAuthorizationResource.class)
                .registerClasses(RecordNotExistsExceptionMapper.class)
                .registerClasses(RepresentationNotExistsExceptionMapper.class)
                .registerClasses(VersionNotExistsExceptionMapper.class)
                .registerClasses(CannotModifyPersistentRepresentationExceptionMapper.class)
                .registerClasses(CannotPersistEmptyRepresentationExceptionMapper.class)
                .property("contextConfigLocation", "classpath:testContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        mutableAclService = applicationContext.getBean(MutableAclService.class);
    }

    @Test
    public void testAuthorizeOtherUser()
            throws Exception {

    	MutableAcl versionAcl = Mockito.mock(MutableAcl.class);
    	when(mutableAclService.readAclById(Mockito.any(ObjectIdentity.class))).thenReturn(versionAcl);
    	
        Response response = target()
        		.path(
        		"/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}" + "/users/{" + Q_USER_NAME + "}")
        		.request()
                .post(null);
        
        System.out.println(response);

        assertThat(response.getStatus(), is(Response.ok().build().getStatus()));
        verify(mutableAclService, times(1)).readAclById(Mockito.any(ObjectIdentity.class));
        verify(mutableAclService, times(1)).updateAcl(Mockito.any(MutableAcl.class));
        verify(versionAcl, times(1)).insertAce(eq(0), eq(BasePermission.READ),Mockito.any(PrincipalSid.class), eq(true));
        verifyNoMoreInteractions(mutableAclService);
        verifyNoMoreInteractions(versionAcl);
    }
    
    @Test
    public void testGiveReadAccessToEveryone()
            throws Exception {

    	MutableAcl versionAcl = Mockito.mock(MutableAcl.class);
    	when(mutableAclService.readAclById(Mockito.any(ObjectIdentity.class))).thenReturn(versionAcl);
    	
        Response response = target()
        		.path(
        		"/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}" + "/free")
        		.request()
                .post(null);
        
        assertThat(response.getStatus(), is(Response.ok().build().getStatus()));
        verify(mutableAclService, times(1)).readAclById(Mockito.any(ObjectIdentity.class));
        verify(mutableAclService, times(1)).updateAcl(Mockito.any(MutableAcl.class));
        verify(versionAcl, times(1)).insertAce(eq(0), eq(BasePermission.READ),Mockito.any(PrincipalSid.class), eq(true));
        verifyNoMoreInteractions(mutableAclService);
        verifyNoMoreInteractions(versionAcl);
    }
}
