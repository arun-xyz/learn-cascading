package com.flipkart.learn.cascading.plain_copier;

import cascading.avro.AvroScheme;
import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;

import java.util.Map;

/**
 * Created by arun.agarwal on 19/05/17.
 */
public class PlainCopierFlow implements CascadingFlows {
    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {
        GlobHfs sampleInputSource = new GlobHfs((Scheme)new AvroScheme(), options.get("input"));

        Pipe sampleInputPipe = new Pipe("sampleInputPipe");

        sampleInputPipe = new Each(sampleInputPipe, new Fields("searchAttributes"),
                new CustomRowHolder(new Fields("output")), Fields.RESULTS);

        Hfs sampleOutputSink = new Hfs(new TextLine(new Fields("searchAttributes")), options.get("output"), SinkMode.REPLACE);

        return FlowDef.flowDef().setName(options.get("flowName"))
                .addSource(sampleInputPipe, sampleInputSource)
                .addTailSink(sampleInputPipe, sampleOutputSink)
                .setAssertionLevel(AssertionLevel.VALID);
    }
}
