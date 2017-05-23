package com.flipkart.learn.cascading;

import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import com.flipkart.learn.cascading.assemblyjoins.JoinerAssembly;
import com.flipkart.learn.cascading.helpers.JunitTestsHelper;
import com.google.common.base.CharMatcher;
import com.hotels.plunger.Bucket;
import com.hotels.plunger.Data;
import com.hotels.plunger.DataBuilder;
import com.hotels.plunger.Plunger;
import lombok.extern.slf4j.Slf4j;

import static com.flipkart.learn.cascading.helpers.JunitTestsHelper.compareResultsWithGold;

/**
 * Created by arun.agarwal on 22/05/17.
 */
@Slf4j
public class CascadingTrainingTest {

    @org.junit.Before
    public void setUp() throws Exception {
    }

    @org.junit.After
    public void tearDown() throws Exception {
    }

    @org.junit.Test
    public void testInnerJoinAssembly() throws Exception {
            Plunger plunger = new Plunger();
        Data employeeInfoData = new DataBuilder(new Fields("name","place", "salary"))
                .addTuples(JunitTestsHelper.getTuples("/Users/arun.agarwal/projects/learn-cascading/src/test/resources/assembled_joins/employee.info.input", CharMatcher.is(',')))
                .build();

        Data regionAverageData = new DataBuilder(new Fields("region", "average"))
                .addTuples(JunitTestsHelper.getTuples("/Users/arun.agarwal/projects/learn-cascading/src/test/resources/assembled_joins/region.average.output", CharMatcher.BREAKING_WHITESPACE))
                .build();

        Pipe employeeInfoPipe = plunger.newNamedPipe("employeeInfoPipe", employeeInfoData);
        Pipe regionAveragePipe = plunger.newNamedPipe("regionAveragePipe", regionAverageData);

        SubAssembly dataJoinerAssemblyTest = new JoinerAssembly(employeeInfoPipe, regionAveragePipe);

        //Test 1:
        Bucket bucket = plunger.newBucket(new Fields("name", "place", "salary", "region", "average"), dataJoinerAssemblyTest.getTails()[0]);
        compareResultsWithGold(bucket, "/Users/arun.agarwal/projects/learn-cascading/src/test/resources/assembled_joins/inner.join.output.gold", new Fields("name"));

    }

    @org.junit.Test
    public void testOuterJoinAssembly() throws Exception {
        Plunger plunger = new Plunger();
        Data employeeInfoData = new DataBuilder(new Fields("name","place", "salary"))
                .addTuples(JunitTestsHelper.getTuples("/Users/arun.agarwal/projects/learn-cascading/src/test/resources/assembled_joins/employee.info.input", CharMatcher.is(',')))
                .build();

        Data regionAverageData = new DataBuilder(new Fields("region", "average"))
                .addTuples(JunitTestsHelper.getTuples("/Users/arun.agarwal/projects/learn-cascading/src/test/resources/assembled_joins/region.average.output", CharMatcher.BREAKING_WHITESPACE))
                .build();

        Pipe employeeInfoPipe = plunger.newNamedPipe("employeeInfoPipe", employeeInfoData);
        Pipe regionAveragePipe = plunger.newNamedPipe("regionAveragePipe", regionAverageData);

        SubAssembly dataJoinerAssemblyTest = new JoinerAssembly(employeeInfoPipe, regionAveragePipe);
        
        //Test 2:
        Bucket bucket1 = plunger.newBucket(new Fields("name", "place", "salary", "region", "average"), dataJoinerAssemblyTest.getTails()[1]);
        compareResultsWithGold(bucket1, "/Users/arun.agarwal/projects/learn-cascading/src/test/resources/assembled_joins/outer.join.output.gold", new Fields("name"));
    }
}