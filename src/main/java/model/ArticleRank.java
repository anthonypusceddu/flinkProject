package model;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.List;

public class ArticleRank implements Serializable {

    private long timestamp;
    private List<Tuple2<String,Integer>> rank;

    public ArticleRank() {
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public List<Tuple2<String, Integer>> getRank() {
        return rank;
    }

    public void setRank(List<Tuple2<String, Integer>> rank) {
        this.rank = rank;
    }

    @Override
    public String toString() {
        return "Timestamp: " + this.timestamp + "\t"+"Rank: "+ this.rank;
    }
}
