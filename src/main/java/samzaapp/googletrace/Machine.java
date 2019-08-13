package samzaapp.googletrace;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * An ad click event.
 */
public class Machine {
    private static final int TIMESTAMP  = 0;
    private static final int MACHINE_ID = 4;
    private static final int CPU        = 5;
    private static final int MEMORY     = 6;

    public long timestamp;
    public String id;
    public int eventType;
    public String platformId;
    public double cpu;
    public double memory;

    public Machine(
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("id") String id,
            @JsonProperty("eventType") int eventType,
            @JsonProperty("platformId") String platformId,
            @JsonProperty("cpu") double cpu,
            @JsonProperty("memory") double memory) {
        this.id = id;
        this.timestamp = timestamp;
        this.cpu = cpu;
        this.memory = memory;
    }
}