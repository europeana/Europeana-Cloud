package eu.europeana.cloud.service.dps.storm.io;

/**
 * Defines the grouping strategy used when connecting a bolt to the previous
 * component in the topology pipeline.
 */
public enum GroupingType {

    /**
     * Applies a ShuffleGrouping from the previous bolt (or from all spouts if this
     * is the first bolt in the pipeline).
     */
    SHUFFLE,

    /**
     * Applies a FieldGroping from the previous bolt (or from all spouts if this
     * is the first bolt in the pipeline).
     */
    FIELDS,

    /**
     * No grouping is applied between the previous component and this bolt.
     */
    NONE
}