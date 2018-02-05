// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.tensor.evaluation;

import com.yahoo.tensor.TensorType;

/**
 * Provides type information about a context (set of variable bindings).
 *
 * @author bratseth
 */
public interface TypeContext {

    /**
     * Returns the type of the tensor with this name.
     *
     * @return returns the type of the tensor which will be returned by calling getTensor(name)
     *         or null if getTensor will return null.
     */
    TensorType getType(String name);

}
