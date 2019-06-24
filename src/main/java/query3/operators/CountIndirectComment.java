package query3.operators;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CountIndirectComment extends ProcessWindowFunction<
        Tuple4<Integer,Integer,Integer,Integer>, Tuple4<Long, Integer, Integer,Integer>, Integer, TimeWindow> {
    @Override
    public void process(Integer key, Context context, Iterable<Tuple4<Integer,Integer,Integer,Integer>> commentList, Collector<Tuple4<Long, Integer, Integer,Integer>> out) throws Exception {
        out.collect(new Tuple4<>(context.window().getStart(), key,  Lists.newArrayList(commentList.iterator().next()).stream().reduce(0, (a, b) -> (a.f2 == b.f1)? (a.f3+ 1) : a.f3),commentList.iterator().next().f0));
    }
}
