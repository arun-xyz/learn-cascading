package com.flipkart.learn.cascading.group_aggregation;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import lombok.Data;

/**
 * Created by arun.agarwal on 22/05/17.
 */
public class SumAggregator extends BaseOperation<SumAggregator.Context> implements Aggregator<SumAggregator.Context>
{

    public SumAggregator(int numArgs, Fields fieldDeclaration) {
        // numArgs : Number of incoming fields that you will be sending (input field count)
        // fieldDeclaration:  The list of fields in the output.
        super(numArgs, fieldDeclaration);
    }

    @Override
    public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
        Context context = new Context();
        aggregatorCall.setContext(context);
    }

    @Override
    public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {

        double value = aggregatorCall.getArguments().getDouble("salary");
        aggregatorCall.getContext().setSum(aggregatorCall.getContext().getSum() + value);
    }

    @Override
    public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {

        Tuple result = new Tuple();
        result.add(aggregatorCall.getGroup().getString("place"));
        result.add(aggregatorCall.getContext().getSum());
        aggregatorCall.getOutputCollector().add(result);
    }

    @Data
    public class Context {
        double sum = 1.0D;
    }
}
