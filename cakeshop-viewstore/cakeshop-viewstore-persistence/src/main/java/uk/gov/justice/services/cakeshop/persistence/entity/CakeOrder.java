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
@Table(name = "cake_order")
public class CakeOrder implements Serializable {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "recipe_id", nullable = false, insertable = true, updatable = true)
    private UUID recipeId;

    @Column(name = "delivery_date", nullable = false, insertable = true, updatable = true)
    private ZonedDateTime deliveryDate;

    public CakeOrder() {

    }

    public CakeOrder(final UUID orderId, final UUID recipeId, final ZonedDateTime deliveryDate) {
        this.orderId = orderId;
        this.recipeId = recipeId;
        this.deliveryDate = deliveryDate;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getRecipeId() {
        return recipeId;
    }

    public ZonedDateTime getDeliveryDate() {
        return deliveryDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CakeOrder cakeOrder = (CakeOrder) o;

        if (!orderId.equals(cakeOrder.orderId)) return false;
        if (!recipeId.equals(cakeOrder.recipeId)) return false;
        return deliveryDate.equals(cakeOrder.deliveryDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOrderId(), getRecipeId(), getDeliveryDate());
    }
}