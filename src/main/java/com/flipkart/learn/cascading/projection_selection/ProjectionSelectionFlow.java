package com.flipkart.learn.cascading.projection_selection;

import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Retain;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;

import java.util.Map;

/**
 * Created by arun.agarwal on 19/05/17.
 */
public class ProjectionSelectionFlow implements CascadingFlows{
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap sampleInputSource = new FileTap(new TextDelimited(new Fields("name", "place", "salary"), ","), options.get("input"));

        Pipe sampleInputPipe = new Pipe("sampleInputPipe");

        String expression  = "(salary<199)";
        ExpressionFilter expressionFilter = new ExpressionFilter(expression, Double.class);

        Pipe highSalaryPipe = new Each(sampleInputPipe,expressionFilter);

        highSalaryPipe = new Retain(highSalaryPipe, new Fields( "name", "place"));

        Tap sampleOutputSink = new FileTap(new TextLine(), options.get("output"), SinkMode.REPLACE);

        return FlowDef.flowDef().setName(options.get("flowName"))
                .addSource(sampleInputPipe, sampleInputSource)
                .addTailSink(highSalaryPipe, sampleOutputSink)
                .setAssertionLevel(AssertionLevel.VALID);
    }
}