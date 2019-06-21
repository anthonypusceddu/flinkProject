package utils;

import model.Post;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;


/**
 * This generator generates watermarks assuming that elements arrive out of order,
 * but only to a certain degree. The latest elements for a certain timestamp t will arrive
 * at most n milliseconds after the earliest elements for timestamp t.
 */

public class PostTimestampAssigner implements  AssignerWithPunctuatedWatermarks<Post>{

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Post post, long l) {
        return new Watermark(l);
    }

    @Override
    public long extractTimestamp(Post post, long l) {
        return post.getCreateDate();
    }
}
/*public class PostTimestampAssigner implements AssignerWithPeriodicWatermarks<Post> {

    private final long maxOutOfOrderness = 5000; // 5 seconds
    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(Post element, long previousElementTimestamp) {
        long timestamp = element.getApproveDate()*1000;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }


}*/


