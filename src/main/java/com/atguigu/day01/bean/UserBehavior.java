package com.atguigu.day01.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author cc
 * @create 2022/7/5 00052:27
 * @Version 1.0
 */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public class UserBehavior {
        private Long userId;
        private Long itemId;
        private Integer categoryId;
        private String behavior;
        private Long timestamp;
    }


