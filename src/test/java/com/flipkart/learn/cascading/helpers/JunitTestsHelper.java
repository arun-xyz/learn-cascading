package com.flipkart.learn.cascading.helpers;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.hotels.plunger.Bucket;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by arun.agarwal on 15/02/17.
 */
@Slf4j
public class JunitTestsHelper {

    // Get tuples from an input data file.
    public static List<Tuple> getTuples(String filename, final CharMatcher matcher) throws Exception{
        return Files.readLines(new File(filename), Charsets.UTF_8,
                new LineProcessor<List<Tuple>>() {

                    List<Tuple> results = new ArrayList<Tuple>();
                    @Override
                    public boolean processLine(String line) throws IOException {
                        if(!line.startsWith("#")) {
                            String[] data = Iterables.toArray(Splitter.on(matcher)
                                    .omitEmptyStrings()
                                    .split(line), String.class);
                            results.add(new Tuple((Object[])data)); // Working with varargs including already available object param method. so cast for warning
                        }
                        return true;
                    }

                    @Override
                    public List<Tuple> getResult() {
                        return results;
                    }
                });
    }

    public static void compareResultsWithGold(Bucket bucket, String goldFile, Fields sortingFields) throws IOException {
        bucket.result().prettyPrinter().print(); //always print the debug for every compare.
        List<String> goldLines = Files.readLines(new File(goldFile), Charsets.UTF_8);
        List<Tuple> actual = bucket.result().orderBy(sortingFields).asTupleList();
        Tuple[] tuples = actual.toArray(new Tuple[actual.size()]);
        int index = 0;
        for (String line: goldLines) {
            assertEquals(line, tuples[index++].toString());
//            System.out.println(tuples[index++].toString());
//            log.info(line);
        }
    }
}
