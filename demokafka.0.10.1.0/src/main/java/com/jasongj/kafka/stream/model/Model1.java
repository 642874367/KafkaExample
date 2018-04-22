package com.jasongj.kafka.stream.model;

public class Model1 {
    private String k;
    private String v;

    public String getK() {
        return k;
    }

    public void setK(String k) {
        this.k = k;
    }

    public String getV() {
        return v;
    }

    public void setV(String v) {
        this.v = v;
    }

    public Model1(){}
    public Model1(String k, String v) {
        this.k = k;
        this.v = v;
    }
}
