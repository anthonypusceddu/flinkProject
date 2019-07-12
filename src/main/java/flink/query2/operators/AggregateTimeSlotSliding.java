package flink.query2.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.TreeMap;

public class AggregateTimeSlotSliding implements AllWindowFunction<Tuple2<Integer, Integer>, Tuple2<Long, Map<Integer, Integer>>, TimeWindow>{
    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<Integer, Integer>> iterable, Collector<Tuple2<Long, Map<Integer, Integer>>> collector) throws Exception {
        Map<Integer,Integer> treeMap = new TreeMap<>();
        //System.out.println(iterable);
        for( Tuple2< Integer, Integer> t : iterable){
            if (treeMap.containsKey(t.f0)){
                treeMap.put(t.f0,treeMap.get(t.f0) + t.f1);
            }else{
                treeMap.put(t.f0,t.f1);
            }
        }
        collector.collect(new Tuple2<>(timeWindow.getStart(),treeMap));
    }
}
