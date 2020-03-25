CREATE TABLE `w_connect_table` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `format` varchar(256) DEFAULT NULL,
  `db` varchar(256) DEFAULT NULL,
  `options` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;