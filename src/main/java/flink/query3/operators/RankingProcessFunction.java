package flink.query3.operators;import model.Score;import model.State;import org.apache.flink.api.java.tuple.Tuple2;import org.apache.flink.api.java.tuple.Tuple5;import org.apache.flink.configuration.Configuration;import org.apache.flink.streaming.api.functions.ProcessFunction;import org.apache.flink.util.Collector;import utils.Config;import java.io.BufferedWriter;import java.io.FileWriter;import java.io.IOException;import java.util.*;import java.util.stream.Collectors;public class RankingProcessFunction extends ProcessFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple2<Long,List<HashMap<Integer, Score>>>> {    private State valueState;    private BufferedWriter bw1;    private BufferedWriter bw2;    private BufferedWriter bw3;    //initialize state    @Override    public void open(Configuration parameters) throws Exception {        super.open(parameters);        valueState = new State(Config.START);        try {            FileWriter writer1 = new FileWriter("result/Query3/query3_day.txt");            FileWriter writer2 = new FileWriter("result/Query3/query3_week.txt");            FileWriter writer3 = new FileWriter("result/Query3/query3_month.txt");            bw1 = new BufferedWriter(writer1);            bw2 = new BufferedWriter(writer2);            bw3 = new BufferedWriter(writer3);        } catch (IOException e) {            System.err.format("IOException: %s%n", e);        }    }    @Override    //input ( UserId, Depth, Like, InReplyTo, CommentID)    public void processElement(Tuple5<Integer, Integer, Integer, Integer, Integer> value, Context ctx, Collector<Tuple2<Long,List<HashMap<Integer,Score>>>> out) throws Exception {        int usrID= value.f0;        int commentId = value.f4;        int replyTo = value.f3;        valueState.setJedis();        /* switch sul valore di depth         se il commento è diretto ( depth 1) allora:              1) verifico che l'utente che ha scritto il commento sia presenta nell'hashmap hUserScore                    1a) se si allora sommo i like del commento diretto ai like che ha gia nella classe score                    1b) se no allora aggiungo l'utente nell'hasmap hUserScore come chiave: id utente, valore: classe score              2) aggiungo nell'hashmap LvL1ToUsrIdMap il collegamento tra commento ed utente                    quindi chiave : CommentoID, valore: UserId          se il commento è indiretto con depth 2 allora:                1) cerco nell Haspmap LvL1ToUsrIdMap, l'id dell'utente del commento a cui il commento appena arrivato è una risposta                2) cerco quell'utente nell'hashmap hUserScore ed aggiungo il count dei commenti indiretti di 1                3) si aggiunge nell'hashmap LvL2ToLvL1Map il riferimento padre figlio tra commendo lvl1 e commento livell2 avendo:                        chiave : commentId ( quindi lvl2) valore : replyto ( quindi commendo id lvl1 )           se il commento è indiretto con depth 3 allora:                1)si ottiene il comment id lvl1 dall'hashmap LvL2ToLvL1Map cercando il valore che ha come chiave repluto del commento                2) tramite l'hashmap LvL1ToUsrIdMap si ottiene l'utente del commento diretto di cui il commendo con l'id ottenuto prima è risposta                3) si aggiunge 1 al count dei commenti indieretti per questo utente         */        switch (value.f1){            case 1:                int like = value.f2;                if ( valueState.usrExist(usrID)) {                    this.valueState.updateLikeScore(usrID,like);                }else {                    this.valueState.addUser(usrID,like);                }                this.valueState.addCommentToUserReference(commentId,usrID);                break;            case 2:                usrID =  this.valueState.retrieveUsrIdFromMap(replyTo);                if ( usrID !=-1) {                    this.valueState.updateCountScore(usrID);                    this.valueState.addCommentToCommentReference(commentId, replyTo);                }                break;            case 3:                commentId = this.valueState.retrieveCommentIdfromMap(replyTo);                usrID = this.valueState.retrieveUsrIdFromMap(commentId);                if ( commentId!= -1 & usrID !=-1) {                    this.valueState.updateCountScore(usrID);                }                break;        }        if (ctx.timestamp()-valueState.getTimestamp() >= Config.H24){            List<HashMap<Integer,Score>> list=this.createRank(valueState.gethUserScoreWindow1());            out.collect(new Tuple2<Long, List<HashMap<Integer, Score>>>(ctx.timestamp(),list));            bw1.write(ctx.timestamp().toString());            bw1.write(list.toString());            bw1.write("\n");            bw1.flush();            valueState.joinHashmap();            valueState.resetWindow1(valueState.getTimestamp()+Config.H24);            valueState.setDay(valueState.getDay()+1);        }        if (valueState.getDay()==7){            List<HashMap<Integer,Score>> list=this.createRank(valueState.gethUserScoreWindow2());            out.collect(new Tuple2<Long, List<HashMap<Integer, Score>>>(ctx.timestamp(),list));            bw2.write(ctx.timestamp().toString());            bw2.write(list.toString());            bw2.write("\n");            bw2.flush();            valueState.resetWindow2();            valueState.setMonth(valueState.getMonth()+1);        }        if (valueState.getMonth()==4){            List<HashMap<Integer,Score>> list=this.createRank(valueState.gethUserScoreWindow3());            out.collect(new Tuple2<Long, List<HashMap<Integer, Score>>>(ctx.timestamp(),list));            bw3.write(ctx.timestamp().toString());            bw3.write(list.toString());            bw3.write("\n");            bw3.flush();            valueState.resetWindow3();        }    }    public List<HashMap<Integer,Score>> createRank(HashMap<Integer,Score> h){        List<HashMap<Integer, Score>> treeMap = new ArrayList<>();        Set list = h.keySet();        Iterator iter = list.iterator();        while (iter.hasNext()) {            Object key = iter.next();            Score value = (Score) h.get(key);            HashMap<Integer,Score> app = new HashMap<>();            app.put((Integer)key,value);            treeMap.add(app);        }        Collections.sort(treeMap, new MyComparator());        treeMap=treeMap.stream().limit(10).collect(Collectors.toList());        return treeMap;    }}