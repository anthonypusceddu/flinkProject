package query3.operators;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.IterableUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.stream.LongStream;

public class CountDirectLike extends ProcessWindowFunction<
        Tuple6<Integer,String,Boolean,Integer,Integer,Integer>, Tuple3<Long, Integer, Integer>, Integer, TimeWindow> {
    @Override
    public void process(Integer key, Context context, Iterable<Tuple6<Integer,String,Boolean,Integer,Integer,Integer>> commentList, Collector<Tuple3<Long, Integer, Integer>> out) throws Exception {


        out.collect(new Tuple3<>(context.window().getStart(), key,  Lists.newArrayList(commentList.iterator().next().f3).stream().reduce(key, Integer::sum)));
    }
}
