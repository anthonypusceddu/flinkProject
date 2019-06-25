package query3.operators;

import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountReplyDepth3 implements WindowFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple3<Long,Integer,Integer>, Integer, TimeWindow> {
    @Override
    public void apply(Integer key, TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple3<Long, Integer, Integer>> out) throws Exception {
        out.collect(new Tuple3<>(timeWindow.getStart(), key, IterableUtils.size(input)));
    }
}
