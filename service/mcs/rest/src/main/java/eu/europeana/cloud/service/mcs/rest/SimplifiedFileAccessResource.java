package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.selectors.LatestPersistentRepresentationVersionSelector;
import eu.europeana.cloud.common.selectors.RepresentationSelector;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.UnitedExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Gives (read) access to files stored in ecloud in simplified (friendly) way. <br/>
 * The latest persistent version of representation is picked up.
 * 
 */
@Path("/data-providers/{" + P_PROVIDER + "}/records/{" + P_LOCALID + "}/representations/{" + P_REPRESENTATIONNAME + "}/{" + P_FILENAME + "}")
@Scope("request")
@Component
public class SimplifiedFileAccessResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimplifiedFileAccessResource.class);

    @Autowired
    private RecordService recordService;

    @Autowired
    private UISClient uisClient;

    @Autowired
    private PermissionEvaluator permissionEvaluator;

    /**
     * Returns file content from <b>latest persistent version</b> of specified representation.
     *
     * @param providerId         providerId
     * @param localId            localId
     * @param representationName representationName
     * @param fileName           fileName
     * @return Requested file context
     * @throws RepresentationNotExistsException
     * @throws FileNotExistsException
     * @throws CloudException
     * @throws RecordNotExistsException
     * @summary Get file content using simplified url
     * @statuscode 204 object has been updated.
     */
    @GET
    public Response getFile(@PathParam(P_PROVIDER) final String providerId,
                            @PathParam(P_LOCALID) final String localId,
                            @PathParam(P_REPRESENTATIONNAME) final String representationName,
                            @PathParam(P_FILENAME) final String fileName)
            throws RepresentationNotExistsException, FileNotExistsException, CloudException, RecordNotExistsException {

        LOGGER.info("Reading file in friendly way for: provider: {}, localId: {}, represenatation: {}, fileName: {}",
                providerId, localId, representationName, fileName);

        final String cloudId = findCloudIdFor(providerId, localId);
        final Representation representation = selectRepresentationVersion(cloudId, representationName);
        if (representation == null) {
            throw new RepresentationNotExistsException();
        }
        if (userHasRightsToReadFile(cloudId, representation.getRepresentationName(), representation.getVersion())) {
            final File requestedFile = readFile(cloudId, representationName, representation.getVersion(), fileName);

            String md5 = requestedFile.getMd5();

            StreamingOutput output = new StreamingOutput() {
                @Override
                public void write(OutputStream output)
                        throws IOException, WebApplicationException {
                    try {
                        final FileResource.ContentRange contentRange = new FileResource.ContentRange(-1L, -1L);
                        recordService.getContent(cloudId, representationName, representation.getVersion(), fileName, contentRange.getStart(), contentRange.getEnd(),
                                output);
                    } catch (RepresentationNotExistsException ex) {
                        throw new WebApplicationException(new UnitedExceptionMapper().toResponse(ex));
                    } catch (FileNotExistsException ex) {
                        throw new WebApplicationException(new UnitedExceptionMapper().toResponse(ex));
                    } catch (WrongContentRangeException ex) {
                        throw new WebApplicationException(new UnitedExceptionMapper().toResponse(ex));
                    }
                }
            };

            return Response.status(Response.Status.OK).entity(output).tag(md5).build();
        } else {
            throw new AccessDeniedException("Access is denied");
        }
    }

    private boolean userHasRightsToReadFile(String cloudId, String representationName, String representationVersion) {

        SecurityContext ctx = SecurityContextHolder.getContext();
        Authentication authentication = ctx.getAuthentication();
        //
        String targetId = cloudId + "/" + representationName + "/" + representationVersion;
        boolean hasAccess = permissionEvaluator.hasPermission(authentication, targetId, Representation.class.getName(), "read");
        return hasAccess;
    }

    private String findCloudIdFor(String providerID, String localId) throws CloudException {
        CloudId foundCloudId = uisClient.getCloudId(providerID, localId);
        return foundCloudId.getId();
    }

    private Representation selectRepresentationVersion(String cloudId, String representationName) throws RepresentationNotExistsException, RecordNotExistsException {
        List<Representation> representations = recordService.listRepresentationVersions(cloudId, representationName);
        RepresentationSelector representationSelector = new LatestPersistentRepresentationVersionSelector();
        return representationSelector.select(representations);
    }

    private File readFile(String cloudId, String representationName, String version, String fileName) throws RepresentationNotExistsException, FileNotExistsException {
        return recordService.getFile(cloudId, representationName, version, fileName);
    }

}
