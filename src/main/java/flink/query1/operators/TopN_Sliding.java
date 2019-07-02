package flink.query1.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class TopN_Sliding implements AllWindowFunction<Tuple2<String, Integer>, Tuple2<Long, List<Tuple2<String, Integer>>>, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<Long,List<Tuple2<String, Integer>>>> collector) throws Exception {
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        HashMap<String,Integer> map = new HashMap<>();
        //System.out.println(iterable);

        for( Tuple2<String, Integer> t : iterable){
            String key = t.f0;
            int value = t.f1;
            if (map.containsKey(key)){
                map.put(key,map.get(key)+value);
            }else {
                map.put(key, value);
            }
        }
        for ( String k : map.keySet()){
            Tuple2<String,Integer> tuple= new Tuple2<>(k,map.get(k));
            list.add(tuple);
        }
        list = list
                .stream()
                .sorted((o1, o2) -> Integer.compare(o2.f1,o1.f1) )
                .limit(3).collect(Collectors.toList());
        collector.collect(new Tuple2<>(timeWindow.getStart(),list));
    }
}
