package com.bettercloud.bigtable.orm.test;

import com.bettercloud.bigtable.orm.annotations.Column;
import com.bettercloud.bigtable.orm.annotations.Entity;
import com.bettercloud.bigtable.orm.annotations.KeyComponent;

@Entity(keyComponents = {
        @KeyComponent(constant = "test")
})
class NoParent {

    @Column(family = "family", qualifier = "qualifier")
    private String value;
}
