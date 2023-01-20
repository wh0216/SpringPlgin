package com.wh.es.entity;

import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LocationPo {

    @ApiModelProperty(value = "经度", required = true)
    private double lon;

    @ApiModelProperty(value = "纬度", required = true)
    private double lat;
}

