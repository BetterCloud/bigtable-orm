package com.bettercloud.bigtable.orm.test;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;
import com.bettercloud.bigtable.orm.annotations.Table;

@Table("table")
class NonPrivateEntity {

    @Entity(keyComponents = {
            @KeyComponent(constant = "test")
    })
    class MyEntity {

        @Column(family = "family", qualifier = "qualifier")
        private String value;
    }
}
