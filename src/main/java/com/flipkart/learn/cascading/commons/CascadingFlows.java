package com.flipkart.learn.cascading.commons;

import cascading.flow.FlowDef;

import java.util.Map;

/**
 * Created by arun.agarwal on 19/05/17.
 */
public interface CascadingFlows {

    //Single method which submits a run with the flow.
    FlowDef getFlowDefinition(Map<String, String> options);
}
