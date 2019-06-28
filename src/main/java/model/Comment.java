package model;

public class Comment {

    private int userID;
    private int commentID;
    private int depth;
    private int inReplyTo;
    private int recommendations;
    private int count;


    public Comment(int userID, int commentID, int depth, int inReplyTo, int recommendations) {
        this.userID = userID;
        this.commentID = commentID;
        this.depth = depth;
        this.inReplyTo = inReplyTo;
        this.recommendations = recommendations;
    }

    public Comment() {
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }

    public int getCommentID() {
        return commentID;
    }

    public void setCommentID(int commentID) {
        this.commentID = commentID;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getInReplyTo() {
        return inReplyTo;
    }

    public void setInReplyTo(int inReplyTo) {
        this.inReplyTo = inReplyTo;
    }

    public int getRecommendations() {
        return recommendations;
    }

    public void setRecommendations(int recommendations) {
        this.recommendations = recommendations;
    }
}
