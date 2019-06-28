package model;

import java.util.HashMap;

public class State {
    //key: commentId, value: comment class {userId,commentId,depth,inReplyTo,like,count}
    private HashMap<Integer,Comment> hdirectComment = new HashMap<>();

    //key: commentId of depht 2 , value: inReplyTo
    private HashMap<Integer,Integer> hindirectComment = new HashMap<>() ;
    private Long timestamp=0L;
    private boolean isFirstTimeInWindow= false;

    public boolean isFirstTimeInWindow() {
        return isFirstTimeInWindow;
    }

    public void setFirstTimeInWindow(boolean firstTimeInWindow) {
        isFirstTimeInWindow = firstTimeInWindow;
    }

    public HashMap<Integer, Comment> getHdirectComment() {
        return hdirectComment;
    }

    public void updateHashmapDirect(Integer key, Comment value){
        this.hdirectComment.put(key,value);
    }

    public void updateHashmapIndirect(Integer key, Integer value){
        this.hindirectComment.put(key,value);
    }

    public void add(Integer key){
        Comment comment=this.hdirectComment.get(key);
        if (comment != null){
            comment.setCount(comment.getCount()+1);
            this.hdirectComment.replace(key,comment);
        }
    }

    public Integer getIdIndirectHash(Integer key){
        return this.hindirectComment.get(key);
    }

    public void setHdirectComment(HashMap<Integer, Comment> hdirectComment) {
        this.hdirectComment = hdirectComment;
    }

    public HashMap<Integer, Integer> getHindirectComment() {
        return hindirectComment;
    }

    public void setHindirectComment(HashMap<Integer, Integer> hindirectComment) {
        this.hindirectComment = hindirectComment;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
