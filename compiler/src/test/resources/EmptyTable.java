package com.bettercloud.bigtable.orm.test;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;

@Table("")
class EmptyTable {

    @Entity(keyComponents = {
        @KeyComponent(constant = "test")
    })
    private class MyEntity {

        @Column(family = "family", qualifier = "qualifier")
        private String value;
    }
}
