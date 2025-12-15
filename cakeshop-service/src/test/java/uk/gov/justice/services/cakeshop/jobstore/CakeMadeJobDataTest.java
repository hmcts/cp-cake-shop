package uk.gov.justice.services.cakeshop.jobstore;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CakeMadeJobDataTest {

    @Test
    void equalsWhenCakeIdIsSame() {
        final CakeMadeJobData cakeMadeJobData1= new CakeMadeJobData("cakeId");
        final CakeMadeJobData cakeMadeJobData2 = new CakeMadeJobData("cakeId");

        assertTrue(cakeMadeJobData1.equals(cakeMadeJobData2));
    }

    @Test
    void notEqualWhenCakeIdIsDifferent() {
        final CakeMadeJobData cakeMadeJobData1= new CakeMadeJobData("cakeId-1");
        final CakeMadeJobData cakeMadeJobData2 = new CakeMadeJobData("cakeId-2");

        assertFalse(cakeMadeJobData1.equals(cakeMadeJobData2));
    }
}