package flink.query1.operators;

import model.ArticleRank;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TopN implements AllWindowFunction<Tuple2< String, Integer>, ArticleRank, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2< String, Integer>> iterable, Collector<ArticleRank> collector) throws Exception {
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        for( Tuple2<String, Integer> t : iterable){
            list.add(new Tuple2<>(t.f0,t.f1));
        }
        list = list
                .stream()
                .sorted((o1, o2) -> Integer.compare(o2.f1,o1.f1) )
                .limit(3).collect(Collectors.toList());
        ArticleRank rank  = new ArticleRank();
        rank.setTimestamp(timeWindow.getEnd());
        rank.setRank(list);
        collector.collect(rank);
    }
}
