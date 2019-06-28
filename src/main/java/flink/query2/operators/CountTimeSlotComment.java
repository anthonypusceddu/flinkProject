package flink.query2.operators;

import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountTimeSlotComment implements WindowFunction<Tuple2<Integer, Integer>, Tuple3<Long, Integer, Integer>, Integer, TimeWindow> {

    @Override
    public void apply(Integer key, TimeWindow timeWindow, Iterable<Tuple2<Integer, Integer>> articleList, Collector<Tuple3<Long, Integer, Integer>> out) throws Exception {
        out.collect(new Tuple3<>(timeWindow.getStart(), key, IterableUtils.size(articleList)));

    }
}
