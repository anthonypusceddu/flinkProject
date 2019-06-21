package query2.operators;

import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountTimeSlotComment extends ProcessWindowFunction<
        Tuple2<Integer,Integer>, Tuple3<Long, Integer, Integer>, Integer, TimeWindow> {
    @Override
    public void process(Integer key, Context context, Iterable<Tuple2<Integer,Integer>> articleList, Collector<Tuple3<Long, Integer, Integer>> out) throws Exception {
        out.collect(new Tuple3<>(context.window().getStart(), key, IterableUtils.size(articleList)));
    }

}
