package uk.gov.justice.services.cakeshop.persistence.entity;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "index")
public class Index implements Serializable {

    @Id
    @Column(name = "index_id")
    private UUID indexId;

    @Column(name = "delivery_date", nullable = false, insertable = true, updatable = true)
    private ZonedDateTime deliveryDate;

    public Index(final UUID indexId, final ZonedDateTime deliveryDate) {
        this.indexId = indexId;
        this.deliveryDate = deliveryDate;
    }

    public Index() {

    }

    public UUID getIndexId() {
        return indexId;
    }

    public ZonedDateTime getDeliveryDate() {
        return deliveryDate;
    }


    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Index index = (Index) o;
        return indexId.equals(index.indexId) &&
                deliveryDate.equals(index.deliveryDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexId, deliveryDate);
    }
}
