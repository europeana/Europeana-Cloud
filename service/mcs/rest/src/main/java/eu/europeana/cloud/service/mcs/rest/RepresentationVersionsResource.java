package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.CLOUD_ID;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;

//http://localhost:8080/mcs/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions
//https://test.ecloud.psnc.pl/api/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions

/**
 * Resource to manage representation versions.
 */
@RestController
@RequestMapping(RepresentationVersionsResource.CLASS_MAPPING)
@Scope("request")
public class RepresentationVersionsResource {

    public static final String CLASS_MAPPING = "/records/{"+CLOUD_ID+"}/representations/{"+REPRESENTATION_NAME+"}/versions";

    @Autowired
    private RecordService recordService;

    /**
     * Lists all versions of record representation. Temporary versions will be
     * included in the returned list.
     * @summary get all representation versions.
     *
     * @return list of all the representation versions.
     * @throws RepresentationNotExistsException representation does not exist.
     */
    @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    public @ResponseBody List<Representation> listVersions(
            final HttpServletRequest request,
            @PathVariable(CLOUD_ID) String cloudId,
            @PathVariable(REPRESENTATION_NAME) String representationName)
            throws RepresentationNotExistsException {

        List<Representation> representationVersions = recordService.listRepresentationVersions(cloudId, representationName);
        for (Representation representationVersion : representationVersions) {
            EnrichUriUtil.enrich(request, representationVersion);
        }

        return representationVersions;
    }
}
