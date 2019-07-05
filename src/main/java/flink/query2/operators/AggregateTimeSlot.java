package flink.query2.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class AggregateTimeSlot implements AllWindowFunction<Tuple3<Long, Integer, Integer>, Tuple2<Long, Map<Integer,Integer> >, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple3<Long, Integer, Integer>> iterable, Collector<Tuple2<Long, Map<Integer,Integer> >> collector) throws Exception {

       //TreeMap per inserire in modo ordinato -- usa alberi red-black
        Map<Integer,Integer> treeMap = new TreeMap<>();

        for( Tuple3<Long, Integer, Integer> t : iterable){
            treeMap.put(t.f1,t.f2);
        }

        //HashMap<Integer,Integer> map = new HashMap<>();
        collector.collect(new Tuple2<>(timeWindow.getStart(),treeMap));
    }
}
