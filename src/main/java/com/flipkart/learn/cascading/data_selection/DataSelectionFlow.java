package com.flipkart.learn.cascading.data_selection;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;

import java.util.Map;

/**
 * Created by arun.agarwal on 24/05/17.
 */
public class DataSelectionFlow implements CascadingFlows {
    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap inputSource = new FileTap(new TextDelimited(new Fields("name", "place", "salary"), ","), options.get("inputpath"));

//        Pipe inputPipe = new Pipe("inputPipe");

//        String expression = "(salary<120)";
//        ExpressionFilter expressionFilter = new ExpressionFilter(expression, Double.class);


        String expression = "((salary>120)?(double)salary*0.1:(double)salary*0.5)";
        ExpressionFunction expressionFilter = new ExpressionFunction(new Fields("bonus"), expression, Double.class);


        Pipe inputPipe = new Each("inputPipe", expressionFilter, Fields.ALL);
        Tap outputSink = new FileTap(new TextDelimited(new Fields("name", "salary", "bonus")), options.get("outputpath"));

        return FlowDef.flowDef().addSource(inputPipe, inputSource)
                .addTailSink(inputPipe, outputSink);
    }
}
