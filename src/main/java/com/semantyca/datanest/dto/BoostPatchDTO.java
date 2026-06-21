package com.semantyca.datanest.dto;

public class BoostPatchDTO {
    private Integer boost;
    private String type; // "brand" or "shared"

    public Integer getBoost() { return boost; }
    public void setBoost(Integer boost) { this.boost = boost; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
}
