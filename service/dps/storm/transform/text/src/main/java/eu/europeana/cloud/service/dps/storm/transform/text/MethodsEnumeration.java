package eu.europeana.cloud.service.dps.storm.transform.text;

/**
 * Interface for enumerations with extract methods.
 * @param <T> Enumeration type
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public interface MethodsEnumeration <T extends MethodsEnumeration<T>>
{
    /**
     * Retrieve the enum constant of extraction method. 
     * If not possible to recognize the enum name, than it will be used default enum.
     * @param value String with potential enum constant (accept also null and unknown string)
     * @return enum constant
     */
    public T getMethod(String value);
    
    public String name();
}
