package org.trueno.elasticsearch.spark.connector;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

/**
 * Created by maverick on 3/19/17.
 */
public class Vertex {

    private String id;
    private Long value;

    public Vertex(){
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public String getId() {

        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
