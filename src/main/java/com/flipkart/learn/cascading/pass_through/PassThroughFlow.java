package com.flipkart.learn.cascading.pass_through;

import cascading.flow.FlowDef;
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
public class PassThroughFlow implements CascadingFlows {
    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap inputSource = new FileTap(new TextDelimited(new Fields("name", "place", "salary"), ","), options.get("inputpath"));

        Pipe inputPipe = new Pipe("inputPipe");

        Tap outputSink = new FileTap(new TextDelimited(new Fields("name", "place", "salary"), "\t"), options.get("outputpath"));

        return FlowDef.flowDef().addSource(inputPipe, inputSource)
                .addTailSink(inputPipe, outputSink);
    }
}
