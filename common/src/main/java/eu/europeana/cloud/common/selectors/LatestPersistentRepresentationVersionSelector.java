package eu.europeana.cloud.common.selectors;

import com.eaio.uuid.UUID;
import eu.europeana.cloud.common.model.Representation;
import java.util.List;

/**
 * Selects latest persistent representation from list of provided representations
 */
public class LatestPersistentRepresentationVersionSelector implements RepresentationSelector {

  @Override
  public Representation select(List<Representation> representations) {

    Representation representationToBeReturned = null;

    for (Representation representation : representations) {
        if (representation.isPersistent()) {
            representationToBeReturned = getRepresentation(representationToBeReturned, representation);
        }
    }
    return representationToBeReturned;
  }

    private static Representation getRepresentation(Representation representationToBeReturned, Representation representation) {
        if (representationToBeReturned != null) {
            UUID uuid = new UUID(representation.getVersion());
            UUID uuid_1 = new UUID(representationToBeReturned.getVersion());
            if (uuid.compareTo(uuid_1) > 0) {
                representationToBeReturned = representation;
            }
        } else {
            representationToBeReturned = representation;
        }
        return representationToBeReturned;
    }
}
