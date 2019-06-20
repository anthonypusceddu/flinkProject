package model;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class PostSchema implements DeserializationSchema<Post>, SerializationSchema<Post> {
    @Override
    public Post deserialize(byte[] bytes) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Post post = null;
        try {
            post = mapper.readValue(bytes, Post.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return post;
    }

    @Override
    public boolean isEndOfStream(Post post) {
        return false;
    }

    @Override
    public byte[] serialize(Post post) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(post).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public TypeInformation<Post> getProducedType() {
        return TypeInformation.of(Post.class);
    }
}
