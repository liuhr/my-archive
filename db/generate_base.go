package db

/*
CREATE TABLE `archive_table_config` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `source_db` varchar(64) NOT NULL,
  `source_table` varchar(64) NOT NULL,
  `archive_type` int(10) unsigned NOT NULL DEFAULT '1',
  `time_field` varchar(64) NOT NULL DEFAULT '',
  `where_field` varchar(64) NOT NULL DEFAULT '',
  `task_server` varchar(64) NOT NULL DEFAULT '',
  `task_time` varchar(32) NOT NULL COMMENT '归档调度的时间,参照crontab',
  `max_slave_delay` int(10) unsigned NOT NULL DEFAULT '10',
  `db_load` varchar(256) NOT NULL DEFAULT 'Threads_running=50',
  `batch_delete_rows` bigint(20) unsigned NOT NULL DEFAULT '5000',
  `max_delete_row` bigint(20) unsigned NOT NULL DEFAULT '1000000000',
  `online_keep_days` int(10) unsigned NOT NULL DEFAULT '0',
  `offline_keep_days` int(10) unsigned NOT NULL DEFAULT '0',
  `start_time` varchar(32) NOT NULL DEFAULT '',
  `stop_time` varchar(32) NOT NULL DEFAULT '',
  `table_parallel` int(10) unsigned NOT NULL DEFAULT '1',
  `access_require` int(11) NOT NULL DEFAULT '1' COMMENT '1:开关打开,2:开关关闭',
  `is_delete` int(11) NOT NULL DEFAULT '0',
  `created_by` varchar(64) NOT NULL DEFAULT 'sys',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `data_dump_path` varchar(128) NOT NULL DEFAULT '/tmp',
  `is_save_data` int(10) unsigned NOT NULL DEFAULT '1' COMMENT '删除归档数据前是否做备份,1:备份,0:不备份',
  PRIMARY KEY (`id`),
  KEY `update_time` (`update_time`),
  KEY `source_table` (`source_table`,`source_db`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;


CREATE TABLE `archive_table_meta_version` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `archive_id` bigint(20) unsigned NOT NULL,
  `table_meta_checksum` varchar(64) DEFAULT NULL,
  `table_meta_text` text,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `schema_name` varchar(64) NOT NULL,
  `table_name` varchar(64) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `table_name` (`table_name`,`schema_name`,`archive_id`),
  KEY `archive_id` (`archive_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `archive_table_log` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `archive_id` bigint(20) unsigned NOT NULL,
  `task_start_time` datetime DEFAULT NULL,
  `task_end_time` datetime DEFAULT NULL,
  `task_status` varchar(64) DEFAULT NULL,
  `total_archive_rows` bigint(20) unsigned DEFAULT NULL,
  `total_archive_byte` bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `archive_id` (`archive_id`),
  KEY `task_start_time` (`task_start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `archive_table_detail_log` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `archive_table_log_id` bigint(20) unsigned NOT NULL,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `archive_log` text,
  `archive_status` varchar(64) DEFAULT NULL,
  `batch_archive_rows` bigint(20) unsigned DEFAULT NULL,
  `table_meta_version_id` bigint(20) unsigned NOT NULL,
  `archive_data_path` varchar(256) DEFAULT NULL,
  `is_delete` int(11) NOT NULL DEFAULT '0',
  `archive_byte` bigint(20) unsigned DEFAULT NULL,
  `schema_name` varchar(64) NOT NULL,
  `table_name` varchar(64) NOT NULL,
  `archive_version` bigint(20) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `archive_table_log_id` (`archive_table_log_id`),
  KEY `start_time` (`start_time`),
  KEY `table_meta_version_id` (`table_meta_version_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

alter table archive_table_detail_log drop key start_time,add key (start_time,is_delete);
*/
