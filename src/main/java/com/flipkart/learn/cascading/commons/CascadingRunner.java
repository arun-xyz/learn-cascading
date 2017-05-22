package com.flipkart.learn.cascading.commons;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Created by arun.agarwal on 18/05/17.
 */
@Slf4j
public class CascadingRunner {

    public static void main(String[] args) {
        //In this series, we would get what command is getting passed and
        // we will get a runner from the Flow factory for that specific case.
        Map<String, String> options = new HashMap<String, String>();

        List<String> list = Arrays.asList(args);

        list.forEach(arg -> options.put(arg.split("=")[0],arg.split("=")[1]));
        log.debug(""+options);
        log.debug(""+options.get("flowName"));

        // Now lets get the appropriate Flow from Flow Factory.
        CascadingFlows cascadingFlow = CascadingFlowFactory.getCascadingFlow(options.get("flowName"));
        FlowDef flowDef = cascadingFlow.getFlowDefinition(options);

//        final Properties properties = CascadingJobConfiguration.getConfiguration(500);
        final Properties properties = new Properties();

        // Run the flow
        LocalFlowConnector flowConnector = new LocalFlowConnector(properties);
        Flow wcFlow = flowConnector.connect(flowDef);
        wcFlow.complete();
    }
}
