package com.jasongj.kafka.stream.model;

public class Model2 {
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

    public Model2(){}
    public Model2(String k, String v) {
        this.k = k;
        this.v = v;
    }
}
