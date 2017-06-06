package com.flipkart.learn.cascading.various_joins;

import cascading.flow.FlowDef;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.OuterJoin;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.commons.CascadingFlows;

import java.util.Map;

/**
 * Created by arun.agarwal on 22/05/17.
 */
public class VariousJoinsFlow implements CascadingFlows{
    @Override
    public FlowDef getFlowDefinition(Map<String, String> options) {

        Tap incomingEmployeeInfoTap = new FileTap(new TextDelimited(new Fields("name","place", "salary"), ","), options.get("employeeInformationInput"));
        Tap incomingRegionAverageTap = new FileTap(new TextDelimited(new Fields("region", "average")), options.get("regionAverageInput"));

        Pipe incomingEmployeeInfoPipe = new Pipe("incomingEmployeeInfoPipe");
        Pipe incomingRegionAveragePipe = new Pipe("incomingRegionAveragePipe");

        Pipe innerJoinedEmployeeAveragePipe = new CoGroup(incomingEmployeeInfoPipe, new Fields("place"),
                                                     incomingRegionAveragePipe, new Fields("region"),
                                                     new Fields("name", "place", "salary", "region", "average"),
                new InnerJoin());

        Pipe outerjoinEmployeeInputPipe = new Pipe("outerJoinEmpPipe", incomingEmployeeInfoPipe);
        Pipe outerjoinRegionAveragePipe = new Pipe("outerJoinRegAvgPipe",incomingRegionAveragePipe);

        Pipe outerJoinedEmployeeAveragePipe = new CoGroup(outerjoinEmployeeInputPipe, new Fields("place"),
                outerjoinRegionAveragePipe, new Fields("region"),
                new Fields("name", "place", "salary", "region", "average"),
                new OuterJoin());


        Tap innerJoinedInformation = new FileTap(new TextDelimited(new Fields("name", "place", "salary", "average"), ","), options.get("innerJoinOutput"), SinkMode.REPLACE);
        Tap outerJoinedInformation = new FileTap(new TextDelimited(new Fields("name", "place", "salary", "average"), ","), options.get("outerJoinOutput"), SinkMode.REPLACE);

        return FlowDef.flowDef().addSource(incomingEmployeeInfoPipe, incomingEmployeeInfoTap)
                .addSource(incomingRegionAveragePipe, incomingRegionAverageTap )
                .addTailSink(innerJoinedEmployeeAveragePipe, innerJoinedInformation)
                .addTailSink(outerJoinedEmployeeAveragePipe, outerJoinedInformation);
    }
}