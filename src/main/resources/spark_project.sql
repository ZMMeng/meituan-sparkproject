/*
Navicat MySQL Data Transfer

Source Server         : hadoop_3306
Source Server Version : 50718
Source Host           : hadoop:3306
Source Database       : spark_project

Target Server Type    : MYSQL
Target Server Version : 50718
File Encoding         : 65001

Date: 2017-07-27 00:10:20
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for ad_black_list
-- ----------------------------
DROP TABLE IF EXISTS `ad_black_list`;
CREATE TABLE `ad_black_list` (
  `user_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of ad_black_list
-- ----------------------------

-- ----------------------------
-- Table structure for ad_click_trend
-- ----------------------------
DROP TABLE IF EXISTS `ad_click_trend`;
CREATE TABLE `ad_click_trend` (
  `date` varchar(30) DEFAULT NULL,
  `hour` varchar(30) DEFAULT NULL,
  `minute` varchar(30) DEFAULT NULL,
  `ad_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of ad_click_trend
-- ----------------------------

-- ----------------------------
-- Table structure for ad_province_top3
-- ----------------------------
DROP TABLE IF EXISTS `ad_province_top3`;
CREATE TABLE `ad_province_top3` (
  `date` varchar(30) DEFAULT NULL,
  `province` varchar(100) DEFAULT NULL,
  `ad_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of ad_province_top3
-- ----------------------------

-- ----------------------------
-- Table structure for ad_stat
-- ----------------------------
DROP TABLE IF EXISTS `ad_stat`;
CREATE TABLE `ad_stat` (
  `date` varchar(30) DEFAULT NULL,
  `province` varchar(100) DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `ad_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of ad_stat
-- ----------------------------

-- ----------------------------
-- Table structure for ad_user_click_count
-- ----------------------------
DROP TABLE IF EXISTS `ad_user_click_count`;
CREATE TABLE `ad_user_click_count` (
  `date` varchar(30) DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  `ad_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of ad_user_click_count
-- ----------------------------

-- ----------------------------
-- Table structure for area_top3_product
-- ----------------------------
DROP TABLE IF EXISTS `area_top3_product`;
CREATE TABLE `area_top3_product` (
  `task_id` int(11) DEFAULT NULL,
  `area` varchar(255) DEFAULT NULL,
  `area_level` varchar(255) DEFAULT NULL,
  `product_id` int(11) DEFAULT NULL,
  `city_names` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  `product_name` varchar(255) DEFAULT NULL,
  `product_status` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of area_top3_product
-- ----------------------------

-- ----------------------------
-- Table structure for city_info
-- ----------------------------
DROP TABLE IF EXISTS `city_info`;
CREATE TABLE `city_info` (
  `city_id` int(11) DEFAULT NULL,
  `city_name` varchar(255) DEFAULT NULL,
  `area` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of city_info
-- ----------------------------
INSERT INTO `city_info` VALUES ('0', '北京', '华北');
INSERT INTO `city_info` VALUES ('1', '上海', '华东');
INSERT INTO `city_info` VALUES ('2', '南京', '华东');
INSERT INTO `city_info` VALUES ('3', '广州', '华南');
INSERT INTO `city_info` VALUES ('4', '三亚', '华南');
INSERT INTO `city_info` VALUES ('5', '武汉', '华中');
INSERT INTO `city_info` VALUES ('6', '长沙', '华中');
INSERT INTO `city_info` VALUES ('7', '西安', '西北');
INSERT INTO `city_info` VALUES ('8', '成都', '西南');
INSERT INTO `city_info` VALUES ('9', '哈尔滨', '东北');

-- ----------------------------
-- Table structure for page_split_convert_rate
-- ----------------------------
DROP TABLE IF EXISTS `page_split_convert_rate`;
CREATE TABLE `page_split_convert_rate` (
  `task_id` int(11) DEFAULT NULL,
  `convert_rate` varchar(255) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of page_split_convert_rate
-- ----------------------------

-- ----------------------------
-- Table structure for session_aggr_stat
-- ----------------------------
DROP TABLE IF EXISTS `session_aggr_stat`;
CREATE TABLE `session_aggr_stat` (
  `task_id` int(11) NOT NULL,
  `session_count` int(11) DEFAULT NULL,
  `1s_3s` double DEFAULT NULL,
  `4s_6s` double DEFAULT NULL,
  `7s_9s` double DEFAULT NULL,
  `10s_30s` double DEFAULT NULL,
  `30s_60s` double DEFAULT NULL,
  `1m_3m` double DEFAULT NULL,
  `3m_10m` double DEFAULT NULL,
  `10m_30m` double DEFAULT NULL,
  `30m` double DEFAULT NULL,
  `1_3` double DEFAULT NULL,
  `4_6` double DEFAULT NULL,
  `7_9` double DEFAULT NULL,
  `10_30` double DEFAULT NULL,
  `30_60` double DEFAULT NULL,
  `60` double DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of session_aggr_stat
-- ----------------------------

-- ----------------------------
-- Table structure for session_detail
-- ----------------------------
DROP TABLE IF EXISTS `session_detail`;
CREATE TABLE `session_detail` (
  `task_id` int(11) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `page_id` int(11) DEFAULT NULL,
  `action_time` varchar(255) DEFAULT NULL,
  `search_keyword` varchar(255) DEFAULT NULL,
  `click_category_id` int(11) DEFAULT NULL,
  `click_product_id` int(11) DEFAULT NULL,
  `order_category_ids` varchar(255) DEFAULT NULL,
  `order_product_ids` varchar(255) DEFAULT NULL,
  `pay_category_ids` varchar(255) DEFAULT NULL,
  `pay_product_ids` varchar(255) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of session_detail
-- ----------------------------

-- ----------------------------
-- Table structure for session_random_extract
-- ----------------------------
DROP TABLE IF EXISTS `session_random_extract`;
CREATE TABLE `session_random_extract` (
  `task_id` int(11) NOT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `start_time` varchar(50) DEFAULT NULL,
  `search_keywords` varchar(255) DEFAULT NULL,
  `click_category_ids` varchar(255) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of session_random_extract
-- ----------------------------

-- ----------------------------
-- Table structure for task
-- ----------------------------
DROP TABLE IF EXISTS `task`;
CREATE TABLE `task` (
  `task_id` int(11) NOT NULL AUTO_INCREMENT,
  `task_name` varchar(255) DEFAULT NULL,
  `create_time` varchar(255) DEFAULT NULL,
  `start_time` varchar(255) DEFAULT NULL,
  `finish_time` varchar(255) DEFAULT NULL,
  `task_type` varchar(255) DEFAULT NULL,
  `task_status` varchar(255) DEFAULT NULL,
  `task_param` text,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of task
-- ----------------------------
INSERT INTO `task` VALUES ('1', null, null, null, null, null, null, '{\"startAge\":[\"10\"],\"endAge\":[\"50\"],\"startDate\":[\"2017-07-23\"],\"endDate\":[\"2017-07-24\"]}');
INSERT INTO `task` VALUES ('2', null, null, null, null, null, null, '{\"targetPageFlow\":[\"1,2,3,4,5,6,7,8,9\"],\"startDate\":[\"2017-07-24\"],\"endDate\":[\"2017-07-25\"]}');

-- ----------------------------
-- Table structure for top10_category
-- ----------------------------
DROP TABLE IF EXISTS `top10_category`;
CREATE TABLE `top10_category` (
  `task_id` int(11) NOT NULL,
  `category_id` int(11) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  `order_count` int(11) DEFAULT NULL,
  `pay_count` int(11) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of top10_category
-- ----------------------------

-- ----------------------------
-- Table structure for top10_session
-- ----------------------------
DROP TABLE IF EXISTS `top10_session`;
CREATE TABLE `top10_session` (
  `task_id` int(11) NOT NULL,
  `category_id` int(11) DEFAULT NULL,
  `session_id` varchar(255) DEFAULT NULL,
  `click_count` int(11) DEFAULT NULL,
  KEY `idx_task_id` (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of top10_session
-- ----------------------------
SET FOREIGN_KEY_CHECKS=1;
