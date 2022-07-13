package com.atguigu.day01.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author cc
 * @create 2022/7/5 000512:00
 * @Version 1.0
 */

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public class MarketingUserBehavior {
        private Long userId;
        private String behavior;
        private String channel;
        private Long timestamp;
    }

