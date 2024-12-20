package ar.edu.itba.pod.tpe2.client;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
@Getter
public class ClientArguments {
    private final String query;
    private final String addresses;
    private final String city;
    private final String readerType;

    private boolean strictAgencies = false;
    private boolean strictInfractions = false;

    private final Map<String, String> optargs;

    private static final Set<String> VALID_CITIES = Set.of("chi", "nyc");

    public ClientArguments() {
        this.query = Validate.notBlank(System.getProperty("query"));
        this.addresses = Validate.notBlank(System.getProperty("addresses"));
        this.city = Validate.notBlank(System.getProperty("city"));
        Validate.isTrue(VALID_CITIES.contains(city.toLowerCase()), "Invalid city: " + city);
        this.readerType = System.getProperty("readerType") != null ? System.getProperty("readerType"): "";

        this.optargs = new HashMap<>();
        initOptargs();

        setStrict(query);
    }

    private void initOptargs() {
        setOptargValue("from");
        setOptargValue("to");
        setOptargValue("n");
        setOptargValue("agency");
    }

    private void setOptargValue(String prop) {
        final String value = System.getProperty(prop);
        if (StringUtils.isNotBlank(value)) {
            optargs.put(prop, value);
        }
    }

    private void setStrict(final String query) {
        switch (query) {
            case "1":
            case "1A":
                strictAgencies = true;
                strictInfractions = true;
                break;
            case "2":
            case "2A":
                strictAgencies = true;
                break;
            case "4":
            case "4A":
                strictInfractions = true;
                break;
            case "3":
            case "3A":
                strictAgencies = false;
                strictInfractions = false;
                break;
            default:
                throw new IllegalArgumentException("Invalid query: " + query);
        }
    }
}
