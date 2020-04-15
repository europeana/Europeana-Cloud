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

//http://localhost:8080/mcs/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions
//https://test.ecloud.psnc.pl/api/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions

/**
 * Resource to manage representation versions.
 */
@RestController
@RequestMapping("/records/{cloudId}/representations/{representation}/versions")
@Scope("request")
public class RepresentationVersionsResource {

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
            @PathVariable String cloudId,
            @PathVariable String representation)
            throws RepresentationNotExistsException {

        List<Representation> representationVersions = recordService.listRepresentationVersions(cloudId, representation);
        for (Representation representationVersion : representationVersions) {
            EnrichUriUtil.enrich(request, representationVersion);
        }

        return representationVersions;
    }
}
