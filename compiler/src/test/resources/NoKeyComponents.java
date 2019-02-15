package com.bettercloud.bigtable.orm.test;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.Table;

@Table("test")
class NoKeyComponents {

    @Entity(keyComponents = {

    })
    private class MyEntity {

        @Column(family = "family", qualifier = "qualifier")
        private String value;
    }
}
