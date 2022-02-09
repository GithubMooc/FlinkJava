package com.pojo;

import lombok.*;

/**
 * @Author Master
 * @Date 2022/2/9
 * @Time 22:47
 * @Name FlinkJava
 *  * 水位传感器：用于接收水位数据
 *  *
 *  * id:传感器编号
 *  * ts:时间戳
 *  * vc:水位
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}

