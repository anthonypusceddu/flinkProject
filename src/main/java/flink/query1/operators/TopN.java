package flink.query1.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TopN implements AllWindowFunction<Tuple2< String, Integer>, Tuple2<Long, List<Tuple2<String, Integer>>>, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2< String, Integer>> iterable, Collector<Tuple2<Long, List<Tuple2<String, Integer>>>> collector) throws Exception {
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for( Tuple2<String, Integer> t : iterable){
            list.add(new Tuple2<>(t.f0,t.f1));
        }
        list = list
                .stream()
                .sorted((o1, o2) -> Integer.compare(o2.f1,o1.f1) )
                .limit(3).collect(Collectors.toList());
        collector.collect(new Tuple2<>(timeWindow.getStart(),list));
    }
}
