package eu.europeana.cloud.service.dps.index.structure;

/**
 * Structures for calculate TF-IDF.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TfIdf 
{
    private int termFrequency;  //term frequency in the field
    private long documentLength; //document lenght
    private int documentFrequency;  //the number of documents containing the current term
    private long numberOfDocuments;  //total number of documents

    public double getTf()
    {
        return (double)termFrequency/documentLength;
    }
    
    public double getIdf()
    {
        return Math.log10((double)numberOfDocuments/documentFrequency);
    }
    
    public double getTfIdf()
    {
        return getTf() * getIdf();
    }
    
    public int getTermFrequency() 
    {
        return termFrequency;
    }

    public void setTermFrequency(int termFrequency) 
    {
        this.termFrequency = termFrequency;
    }

    public long getDocumentLength() 
    {
        return documentLength;
    }

    public void setDocumentLength(long documentLength) 
    {
        this.documentLength = documentLength;
    }

    public int getDocumentFrequency() 
    {
        return documentFrequency;
    }

    public void setDocumentFrequency(int documentFrequency) 
    {
        this.documentFrequency = documentFrequency;
    }

    public long getNumberOfDocuments() 
    {
        return numberOfDocuments;
    }

    public void setNumberOfDocuments(long numberOfDocuments) 
    {
        this.numberOfDocuments = numberOfDocuments;
    }
}
