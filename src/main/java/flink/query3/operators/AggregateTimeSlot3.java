package flink.query3.operators;

import model.Score;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class AggregateTimeSlot3 implements AllWindowFunction< Tuple2<Long, HashMap<Integer,Score>>,  Tuple2<Long, List<HashMap<Integer,Score>> >, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<Long,HashMap<Integer,Score>>> iterable, Collector<  Tuple2<Long, List<HashMap<Integer,Score>>>> collector) throws Exception {

        List<HashMap<Integer, Score>> treeMap = new ArrayList<>();

        for( Tuple2<Long,HashMap<Integer,Score>> t : iterable){
            Iterator it =  t.f1.entrySet().iterator();

            while (it.hasNext()) {
                // leggi chiave/valore dal entr

                Map.Entry entry = (Map.Entry)it.next();

                HashMap<Integer,Score> app = new HashMap<>();
                app.put((Integer)entry.getKey(),(Score)entry.getValue());
                treeMap.add(app);
            }
        }

        Collections.sort(treeMap, new MyComparator());
        //treeMap.subList(0,3);

        //System.out.println(treeMap.subList(0,3));

        treeMap=treeMap.stream().limit(10).collect(Collectors.toList());;
        System.out.println(treeMap);
        //HashMap<Integer,Integer> map = new HashMap<>();
        collector.collect(new Tuple2<Long, List<HashMap<Integer,Score>>>(timeWindow.getStart(),treeMap));
    }
}
