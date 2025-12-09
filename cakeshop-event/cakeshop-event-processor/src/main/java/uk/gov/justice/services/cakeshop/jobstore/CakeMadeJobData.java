package uk.gov.justice.services.cakeshop.jobstore;

import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.HashCodeBuilder;

@SuppressWarnings("squid:S2384")
public class CakeMadeJobData {

    private final String cakeId;

    public CakeMadeJobData(@JsonProperty("cakeId") final String cakeId) {
        this.cakeId = cakeId;
    }

    public String getCakeId() {
        return cakeId;
    }

    public String toString() {
        return "CakeMadeJobData(cakeId=" + this.getCakeId() + ")";
    }

    @Override
    public boolean equals(final Object o) {
        return reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(cakeId).toHashCode();
    }
}
