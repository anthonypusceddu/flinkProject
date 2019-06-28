package flink.query1.operators;

import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountArticleComment implements WindowFunction<Tuple2<String,Integer>, Tuple3<Long, String, Integer>, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> articleList, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
        out.collect(new Tuple3<>(timeWindow.getStart(), key, IterableUtils.size(articleList)));

    }
}
