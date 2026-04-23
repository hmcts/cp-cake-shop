package uk.gov.justice.services.cakeshop.persistence.entity;

import java.io.Serializable;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "cake")
public class Cake implements Serializable {

    @Id
    @Column(name = "cake_id")
    private UUID cakeId;

    @Column(name = "name", nullable = false, insertable = true, updatable = true)
    private String name;


    public Cake(final UUID cakeId, final String name) {
        this.cakeId = cakeId;
        this.name = name;
    }

    public Cake() {

    }

    public UUID getCakeId() {
        return cakeId;
    }

    public String getName() {
        return name;
    }



}