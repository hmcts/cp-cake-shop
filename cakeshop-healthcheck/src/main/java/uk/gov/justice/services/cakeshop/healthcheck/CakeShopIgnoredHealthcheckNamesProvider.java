package uk.gov.justice.services.cakeshop.healthcheck;

import static java.util.Collections.singletonList;
import static uk.gov.justice.services.healthcheck.healthchecks.JobStoreHealthcheck.JOB_STORE_HEALTHCHECK_NAME;

import uk.gov.justice.services.healthcheck.api.DefaultIgnoredHealthcheckNamesProvider;

import java.util.List;

import jakarta.enterprise.inject.Specializes;

@Specializes
public class CakeShopIgnoredHealthcheckNamesProvider extends DefaultIgnoredHealthcheckNamesProvider {

    public CakeShopIgnoredHealthcheckNamesProvider() {}

    @Override
    public List<String> getNamesOfIgnoredHealthChecks() {
        return singletonList(JOB_STORE_HEALTHCHECK_NAME);
    }
}
