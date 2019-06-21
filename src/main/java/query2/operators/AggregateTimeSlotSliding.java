package query2.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AggregateTimeSlotSliding implements AllWindowFunction<Tuple3<Long, Integer, Integer>, Tuple2<Long, Map<Integer, Integer>>, TimeWindow>{
    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple3<Long, Integer, Integer>> iterable, Collector<Tuple2<Long, Map<Integer, Integer>>> collector) throws Exception {
        Map<Integer,Integer> treeMap = new TreeMap<>();
        System.out.println(iterable);
        for( Tuple3<Long, Integer, Integer> t : iterable){
            if (treeMap.containsKey(t.f1)){
                treeMap.put(t.f1,treeMap.get(t.f1) + t.f2);
            }else{
                treeMap.put(t.f1,t.f2);
            }
        }
        collector.collect(new Tuple2<>(timeWindow.getStart(),treeMap));
    }
}
