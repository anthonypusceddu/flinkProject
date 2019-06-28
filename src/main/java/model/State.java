package model;

import java.util.HashMap;

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


    public State(Long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean usrExist(Integer usrID){
        return this.hUserScore.containsKey(usrID);
    }

    public void updateLikeScore(Integer usrID,Integer like){
        Score s = this.hUserScore.get(usrID);
        s.addLike(like);
        this.hUserScore.replace(usrID,s);
    }
    public void updateCountScore(Integer usrID){
        Score s = this.hUserScore.get(usrID);
        s.addCount(1);
        this.hUserScore.replace(usrID,s);
    }

    public void addUser(int usrID,int like) {
        this.hUserScore.put(usrID,new Score(like,0));
    }

    public void addCommentToUserReference(int commentId, int usrID) {
        LvL1ToUsrIdMap.put(commentId,usrID);
    }

   /* public boolean isFirstTimeInWindow() {
        return isFirstTimeInWindow;
    }

    public void setFirstTimeInWindow(boolean firstTimeInWindow) {
        isFirstTimeInWindow = firstTimeInWindow;
    }*/

    public HashMap<Integer, Score> gethUserScore() {
        return hUserScore;
    }

    public void sethUserScore(HashMap<Integer, Score> hUserScore) {
        this.hUserScore = hUserScore;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public HashMap<Integer, Integer> getLvL2ToLvL1Map() {
        return LvL2ToLvL1Map;
    }

    public void setLvL2ToLvL1Map(HashMap<Integer, Integer> lvL2ToLvL1Map) {
        LvL2ToLvL1Map = lvL2ToLvL1Map;
    }

    public HashMap<Integer, Integer> getLvL1ToUsrIdMap() {
        return LvL1ToUsrIdMap;
    }

    public void setLvL1ToUsrIdMap(HashMap<Integer, Integer> lvL1ToUsrIdMap) {
        LvL1ToUsrIdMap = lvL1ToUsrIdMap;
    }


    public int retrieveUsrIdFromMap(int replyTo) {
        return LvL1ToUsrIdMap.get(replyTo);

    }

    public void addCommentToCommentReference(int commentId, int replyTo) {
        LvL2ToLvL1Map.put(commentId,replyTo);
    }

    public int retrieveCommentIdfromMap(int replyTo) {
        return  LvL2ToLvL1Map.get(replyTo);
    }

    public void reset(long l) {
        this.timestamp = l;
        for ( int usr : hUserScore.keySet()){
            Score s = hUserScore.get(usr);
            s.clearScore();
            hUserScore.replace(usr,s);
        }
    }
}
