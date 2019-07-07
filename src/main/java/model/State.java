package model;

import redis.RedisJava;
import redis.clients.jedis.Jedis;
import utils.Deserializer;
import utils.Serializer;

import java.util.HashMap;
import java.util.Map;

public class State {
    //key: commentId_LvL2, value: CommentId_LvL1
    private HashMap<Integer,Integer> LvL2ToLvL1Map = new HashMap<>();
    //key: commentId_LvL1, value: UserID
    private HashMap<Integer,Integer> LvL1ToUsrIdMap = new HashMap<>();
    //key : User_Id, value: user_score( = wa*Like +wb*count)
    private HashMap<Integer,Score> hUserScore = new HashMap<>();
    // first timestamp in window
    private Long timestamp;
    //private boolean isFirstTimeInWindow= true;

    Jedis jedis ;



    public State() {
    }

    public State(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Jedis getJedis() {
        return jedis;
    }

    public void setJedis() throws InterruptedException {
        if(this.jedis==null){
            this.jedis = RedisJava.connect();


            this.jedis.flushAll();
            this.hUserScore.clear();
            this.LvL2ToLvL1Map.clear();
            this.LvL1ToUsrIdMap.clear();
        }


        //Thread.sleep(1000);

    }

    public boolean usrExist(Integer usrID) {
        //controllo se l'utente esiste nella lista locale
        //se non esiste controllo in redis
        //se in redis esiste allora ne faccio una copia nella lista locale
        boolean existsUsr = this.hUserScore.containsKey(usrID);
        if (existsUsr == false){
            boolean exists= this.jedis.hexists("hUserScore", String.valueOf(usrID));
            if (exists==true){
                String r= this.jedis.hget("hUserScore",String.valueOf(usrID));
                Score score=Deserializer.deserialize_score(r);
                hUserScore.put(usrID,score );

            }
            return exists;

      }
        return true;
    }

    public void updateLikeScore(Integer usrID,Integer like){
        //Deserialize
        String r= this.jedis.hget("hUserScore",String.valueOf(usrID));
        Score score=Deserializer.deserialize_score(r);
        score.addLike(like);
        score.calculateScore();
        //serializzo
        String score_serializer=Serializer.serialize_score(score);
        this.jedis.hset("hUserScore",String.valueOf(usrID),score_serializer);


        Score s = this.hUserScore.get(usrID);
        s.addLike(1);
        s.calculateScore();
        this.hUserScore.replace(usrID,s);

    }
    public void updateCountScore(Integer usrID){
        //Deserialize
        String r= this.jedis.hget("hUserScore",String.valueOf(usrID));
        Score score=Deserializer.deserialize_score(r);
        score.addCount(1);
        score.calculateScore();
        //serializzo
        String score_serializer=Serializer.serialize_score(score);
        this.jedis.hset("hUserScore",String.valueOf(usrID),score_serializer);


        Score s = this.hUserScore.get(usrID);

        s.addCount(1);
        s.calculateScore();
        this.hUserScore.replace(usrID,s);


    }

    public void addUser(int usrID,int like) {
        this.hUserScore.put(usrID,new Score(like,0));

        String score_serialized= Serializer.serialize_score(new Score( like,0));
        String str_usrid=String.valueOf(usrID);

        this.jedis.hset("hUserScore",str_usrid,score_serialized);
    }

    public void addCommentToUserReference(int commentId, int usrID) {
        LvL1ToUsrIdMap.put(commentId,usrID);

        String a=String.valueOf(commentId);
        String b= String.valueOf(usrID);
        this.jedis.hset("LvL1ToUsrIdMap",a,b);

    }

    public HashMap<Integer, Score> gethUserScore() {
        return hUserScore;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public int retrieveUsrIdFromMap(int replyTo) {
        //cerco l'utente nella lista locale
        //se non c è lo cerco in redis e lo aggiungo nella lista locale
        int usrId=LvL1ToUsrIdMap.getOrDefault(replyTo, -1);
        if(usrId==-1){
            String str_usrid=this.jedis.hget("LvL1ToUsrIdMap",String.valueOf(replyTo));
            if(str_usrid != null){
                usrId=Integer.valueOf(str_usrid);

                //prendo l'utente da redis e lo aggiungo alla lista di utenti locale
                String r= this.jedis.hget("hUserScore",String.valueOf(str_usrid));
                Score score=Deserializer.deserialize_score(r);
                hUserScore.put(usrId,score);
            }
            return usrId;
        }
        return usrId;
    }

    public void addCommentToCommentReference(int commentId, int replyTo) {
        this.jedis.hset("LvL2ToLvL1Map",String.valueOf(commentId), String.valueOf(replyTo));

        LvL2ToLvL1Map.put(commentId,replyTo);
    }

    public int retrieveCommentIdfromMap(int replyTo) {
        //cerco il commento nella lista locale
        //se non c è lo cerco in redis
        int commentId=LvL2ToLvL1Map.getOrDefault(replyTo, -1);
        if(commentId==-1) {


            String str_commentId = this.jedis.hget("LvL2ToLvL1Map", String.valueOf(replyTo));
            if (str_commentId != null) {
                commentId = Integer.valueOf(str_commentId);
            }
            return commentId;
        }
        return commentId;
    }

    public void reset(long l) {
        this.timestamp = l;
        for ( String usr : this.jedis.hkeys("hUserScore")){
            String r= this.jedis.hget("hUserScore",String.valueOf(usr));

            //deserializer
            Score score_deserialized=Deserializer.deserialize_score(r);
            score_deserialized.clearScore();

            //serializzo
            String score_serialized=Serializer.serialize_score(score_deserialized);
            this.jedis.hset("hUserScore",usr,score_serialized);

        }

        //reset delle 3 hashmap di appoggio
        hUserScore.clear();
        LvL1ToUsrIdMap.clear();
        LvL2ToLvL1Map.clear();

    }


}
