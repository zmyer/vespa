// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.searchlib.rankingexpression.integration.tensorflow;

import com.yahoo.searchlib.rankingexpression.RankingExpression;
import com.yahoo.tensor.TensorType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author lesters
 */
public class DropoutImportTestCase {

    @Test
    public void testDropoutImport() {
        TestableTensorFlowModel model = new TestableTensorFlowModel("src/test/files/integration/tensorflow/dropout/saved");

        // Check (provided) macros
        assertEquals(1, model.get().macros().size());
        assertTrue(model.get().macros().containsKey("training_input"));
        assertEquals("constant(\"training_input\")", model.get().macros().get("training_input").getRoot().toString());

        // Check required macros
        assertEquals(1, model.get().requiredMacros().size());
        assertTrue(model.get().requiredMacros().containsKey("X"));
        assertEquals(new TensorType.Builder().indexed("d0").indexed("d1", 784).build(),
                     model.get().requiredMacros().get("X"));

        TensorFlowModel.Signature signature = model.get().signature("serving_default");

        assertEquals("Has skipped outputs",
                     0, model.get().signature("serving_default").skippedOutputs().size());

        RankingExpression output = signature.outputExpression("y");
        assertNotNull(output);
        assertEquals("outputs/BiasAdd", output.getName());
        assertEquals("join(rename(reduce(join(X, rename(constant(\"outputs_kernel\"), (d0, d1), (d1, d3)), f(a,b)(a * b)), sum, d1), d3, d1), rename(constant(\"outputs_bias\"), d0, d1), f(a,b)(a + b))",
                output.getRoot().toString());
        model.assertEqualResult("X", output.getName());
    }

}
