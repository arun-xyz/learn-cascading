package com.flipkart.learn.cascading.group_aggregation;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Created by arun.agarwal on 22/05/17.
 */
public class SumAggregatorDoubleContext extends BaseOperation<Double> implements Aggregator<Double> {

    public SumAggregatorDoubleContext(int numArgs, Fields fieldDeclaration) {
        super(numArgs, fieldDeclaration);
    }

    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<Double> aggregatorCall) {
        aggregatorCall.setContext(0.0D);
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<Double> aggregatorCall) {
        aggregatorCall.setContext(aggregatorCall.getContext()+Double.valueOf(aggregatorCall.getArguments().getString("salary")));
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<Double> aggregatorCall) {
        Tuple result = new Tuple();
        result.add(aggregatorCall.getGroup().getString("place"));
        result.add(aggregatorCall.getContext());
        aggregatorCall.getOutputCollector().add(result);
    }
}
