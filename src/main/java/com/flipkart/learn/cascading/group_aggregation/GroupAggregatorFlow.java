package com.flipkart.learn.cascading.group_aggregation;

import cascading.flow.FlowDef;
import cascading.operation.AssertionLevel;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
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
public class GroupAggregatorFlow implements CascadingFlows{
    public FlowDef getFlowDefinition(Map<String, String> options) {
        Tap sampleInputSource = new FileTap(new TextDelimited(new Fields("name", "place", "salary"), ","), options.get("input"));

        Pipe sampleInputPipe = new Pipe("sampleInputPipe");

        //Lets do a group by on region and get the average salary.
//        Pipe groupByPipe = new SumBy(sampleInputPipe, new Fields("place"), new Fields("salary"), new Fields("region_salary_average"),Double.class);

        Pipe groupByPipe = new GroupBy(sampleInputPipe, new Fields("place"));

        // Every args :
        // Pipe,   incoming Fields Selector,  Aggregator operation,   outgoing Fields selector.
        // The constructor of aggregator operation takes a fields list - this is how it would affect the outgoing fields,
        // Input tuple to Operation/Agg would be :   Intersection(original tuple fields, argument selector).
        // Output Tuple fields would be:  Intersection ( output fields list,  Union (original tuple, operations fields))

//        groupByPipe = new Every(groupByPipe, new Fields("place", "salary"), new SumAggregator(2, new Fields("place", "salary_average")), Fields.RESULTS);
        groupByPipe = new Every(groupByPipe, new Fields("place", "salary"), new SumAggregatorDoubleContext(2, new Fields("place", "salary_average")), Fields.RESULTS);

        Tap sampleOutputSink = new FileTap(new TextLine(), options.get("output"), SinkMode.REPLACE);

        return FlowDef.flowDef().setName(options.get("flowName"))
                .addSource(sampleInputPipe, sampleInputSource)
                .addTailSink(groupByPipe, sampleOutputSink)
                .setAssertionLevel(AssertionLevel.VALID);
    }
}