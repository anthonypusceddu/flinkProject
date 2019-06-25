package query3.operators;

import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/*public class CountReplyDepth2 implements AllWindowFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple3<Long,Integer, List<Integer>>, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple5<Integer, Integer, Integer, Integer, Integer>> iterable, Collector<Tuple3<Long, Integer, List<Integer>>> out) throws Exception {
        out.collect(new Tuple3<>(timeWindow.getStart(), key, list));

    }
}*/
