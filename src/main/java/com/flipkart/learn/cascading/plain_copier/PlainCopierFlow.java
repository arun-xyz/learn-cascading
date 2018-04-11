package com.flipkart.learn.cascading.plain_copier;

import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import com.flipkart.learn.cascading.commons.CascadingFlows;

import java.util.Map;

/**
 * Created by arun.agarwal on 19/05/17.
 */
public class PlainCopierFlow implements CascadingFlows {
    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap sampleInputSource = new FileTap(new TextLine(), options.get("input"));

        Pipe sampleInputPipe = new Pipe("sampleInputPipe");

        Tap sampleOutputSink = new FileTap(new TextLine(), options.get("output"), SinkMode.REPLACE);

        return FlowDef.flowDef().setName(options.get("flowName"))
                .addSource(sampleInputPipe, sampleInputSource)
                .addTailSink(sampleInputPipe, sampleOutputSink)
                .setAssertionLevel(AssertionLevel.VALID);
    }
}
