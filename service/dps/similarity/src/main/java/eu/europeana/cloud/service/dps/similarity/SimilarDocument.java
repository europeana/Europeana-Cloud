package eu.europeana.cloud.service.dps.similarity;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class SimilarDocument 
{
    private final String referenceDocument;
    private final String assignedDocument;
    private Float score;
    
    public SimilarDocument(String referenceDocument, String assignedDocument) 
    {
        this.referenceDocument = referenceDocument;
        this.assignedDocument = assignedDocument;
    }
    
    public void setScore(float score)
    {
        this.score = score;
    }
    
    public float getScore()
    {
        return score;
    }

    public String getReferenceDocument() 
    {
        return referenceDocument;
    }

    public String getAssignedDocument() 
    {
        return assignedDocument;
    } 
}
