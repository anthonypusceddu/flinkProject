package model;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ArticleRankSchema implements DeserializationSchema<ArticleRank>, SerializationSchema<ArticleRank> {


    @Override
    public ArticleRank deserialize(byte[] bytes) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        ArticleRank article = null;
        try {
            article = mapper.readValue(bytes, ArticleRank.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return article;
    }

    @Override
    public boolean isEndOfStream(ArticleRank articleRank) {
        return false;
    }

    @Override
    public byte[] serialize(ArticleRank articleRank) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(articleRank).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public TypeInformation<ArticleRank> getProducedType() {
        return null;
    }
}
