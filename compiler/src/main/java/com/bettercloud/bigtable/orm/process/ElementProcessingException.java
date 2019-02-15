package com.bettercloud.bigtable.orm.process;

import javax.lang.model.element.Element;

class ElementProcessingException extends Exception {

    private final Element element;

    ElementProcessingException(final String message, final Element element) {
        super(message);

        this.element = element;
    }

    Element getElement() {
        return element;
    }
}
