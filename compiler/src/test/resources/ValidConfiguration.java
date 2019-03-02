package com.bettercloud.bigtable.orm.test;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;

import java.util.UUID;

@Table("table")
class ValidConfiguration {

    @Entity(keyComponents = {
            @KeyComponent(constant = "test"),
            @KeyComponent(name = "name", type = UUID.class)
    })
    private class MyEntity {

        @Column(family = "family", qualifier = "qualifier1")
        private String value1;

        @Column(family = "family")
        private String[] value2;

        @Column(family = "family", qualifier = "qualifier3", versioned = true)
        private int value3;
    }
}
