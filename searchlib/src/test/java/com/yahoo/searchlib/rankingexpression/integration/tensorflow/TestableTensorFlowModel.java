// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.searchlib.rankingexpression.integration.tensorflow;

import com.yahoo.searchlib.rankingexpression.evaluation.Context;
import com.yahoo.searchlib.rankingexpression.evaluation.MapContext;
import com.yahoo.searchlib.rankingexpression.evaluation.TensorValue;
import com.yahoo.tensor.Tensor;
import com.yahoo.tensor.TensorType;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;

import java.nio.FloatBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Helper for TensorFlow import tests: Imports a model and provides asserts on it.
 * This currently assumes the TensorFlow model takes a single input of type tensor(d0[1],d1[784])
 *
 * @author bratseth
 */
public class TestableTensorFlowModel {

    private SavedModelBundle tensorFlowModel;
    private TensorFlowModel model;

    // Sizes of the input vector
    private final int d0Size = 1;
    private final int d1Size = 784;

    public TestableTensorFlowModel(String modelDir) {
        tensorFlowModel = SavedModelBundle.load(modelDir, "serve");
        model = new TensorFlowImporter().importModel(tensorFlowModel);
    }

    public TensorFlowModel get() { return model; }

    public void assertEqualResult(String inputName, String operationName) {
        Tensor tfResult = tensorFlowExecute(tensorFlowModel, inputName, operationName);
        Context context = contextFrom(model);
        Tensor placeholder = placeholderArgument();
        context.put(inputName, new TensorValue(placeholder));
        Tensor vespaResult = model.expressions().get(operationName).evaluate(context).asTensor();
        assertEquals("Operation '" + operationName + "' produces equal results", tfResult, vespaResult);
    }

    private Tensor tensorFlowExecute(SavedModelBundle model, String inputName, String operationName) {
        Session.Runner runner = model.session().runner();
        org.tensorflow.Tensor<?> placeholder = org.tensorflow.Tensor.create(new long[]{ d0Size, d1Size },
                                                                            FloatBuffer.allocate(d0Size * d1Size));
        runner.feed(inputName, placeholder);
        List<org.tensorflow.Tensor<?>> results = runner.fetch(operationName).run();
        assertEquals(1, results.size());
        return TensorConverter.toVespaTensor(results.get(0));
    }

    private Context contextFrom(TensorFlowModel result) {
        MapContext context = new MapContext();
        result.constants().forEach((name, tensor) -> context.put("constant(\"" + name + "\")", new TensorValue(tensor)));
        return context;
    }

    private Tensor placeholderArgument() {
        Tensor.Builder b = Tensor.Builder.of(new TensorType.Builder().indexed("d0", d0Size).indexed("d1", d1Size).build());
        for (int d0 = 0; d0 < d0Size; d0++)
            for (int d1 = 0; d1 < d1Size; d1++)
                b.cell(0, d0, d1);
        return b.build();
    }

}
