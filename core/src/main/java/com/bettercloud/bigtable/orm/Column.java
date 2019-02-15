package com.bettercloud.bigtable.orm;

import com.fasterxml.jackson.core.type.TypeReference;

public interface Column {

    String getFamily();

    String getQualifier();

    TypeReference<?> getTypeReference();
}
