package eu.europeana.cloud.common.model;

import eu.europeana.cloud.common.utils.DateAdapter;
import lombok.*;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.Serializable;
import java.util.Date;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Revision implements Serializable {
    private static final long serialVersionUID = 1L;

    private String revisionName;
    private String revisionProviderId;

    @XmlElement(name = "creationTimeStamp", required = true)
    @XmlJavaTypeAdapter(DateAdapter.class)
    private Date creationTimeStamp;

    boolean acceptance;
    boolean published;
    boolean deleted;

    public Revision(String revisionName, String providerId) {
        this(revisionName, providerId, new Date(), false, false, false);
    }

    public Revision(String revisionName, String providerId, Date creationTimeStamp) {
        this(revisionName, providerId, creationTimeStamp, false, false, false);
    }

    public Revision(final Revision revision) {
        this(revision.getRevisionName(),
                revision.getRevisionProviderId(),
                revision.getCreationTimeStamp(),
                revision.isAcceptance(),
                revision.isPublished(),
                revision.isDeleted()
        );
    }
}
