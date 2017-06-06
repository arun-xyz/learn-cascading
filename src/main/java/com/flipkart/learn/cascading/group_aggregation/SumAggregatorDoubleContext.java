package com.flipkart.learn.cascading.group_aggregation;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;

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

//        aggregatorCall.setContext(aggregatorCall.getContext()+aggregatorCall.getArguments().getString("salary"));
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<Double> aggregatorCall) {

    }
}
