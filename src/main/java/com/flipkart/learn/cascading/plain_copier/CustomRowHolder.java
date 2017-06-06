package com.flipkart.learn.cascading.plain_copier;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Created by arun.agarwal on 01/06/17.
 */
public class CustomRowHolder extends BaseOperation implements Function {
    public CustomRowHolder(Fields output) {
        super(1, output);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        Tuple entry = (Tuple) functionCall.getArguments().getObject(new Fields("searchAttributes"));
        Tuple result = new Tuple();
        if(entry != null) {
//            System.out.println(entry.getString(1));
            result.add(entry.getString(1));
            functionCall.getOutputCollector().add(result);
        }
    }
}
