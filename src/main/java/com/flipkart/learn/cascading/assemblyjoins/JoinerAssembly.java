package com.flipkart.learn.cascading.assemblyjoins;

import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.OuterJoin;
import cascading.tuple.Fields;

/**
 * Created by arun.agarwal on 23/05/17.
 */
public class JoinerAssembly extends SubAssembly {
    public JoinerAssembly(Pipe incomingEmployeeInfoPipe, Pipe incomingRegionAveragePipe) {


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

        setTails(innerJoinedEmployeeAveragePipe, outerJoinedEmployeeAveragePipe);

    }
}
