<?php

namespace LevCharity\Managers;

class LevitDBManager extends BaseManager
{
	protected function addActions()
	{
		add_action('init', [$this, 'create_tables']);
	}

	public function create_tables()
	{
		global $wpdb;
		$donations_table_name = $wpdb->prefix."levcharity_donations";
			$wpdb->query(
				"CREATE TABLE IF NOT EXISTS `{$donations_table_name}` (
				  `ID` int(11) NOT NULL AUTO_INCREMENT,
				  `order_id` int(11) NOT NULL,
				  `order_date` date NOT NULL,
				  `currency` varchar(3) COLLATE utf8mb4_unicode_ci NOT NULL,
				  `total` FLOAT NOT NULL,
				  `order_status` varchar(45) COLLATE utf8mb4_unicode_ci NOT NULL,
				  `conversion_rate` float NOT NULL,
				  `display_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
				  `campaign_message` text COLLATE utf8mb4_unicode_ci,
				  `quantity` int(11) NOT NULL,
				  `anonymous` varchar(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
				  `billing_email` varchar(55) COLLATE utf8mb4_unicode_ci NOT NULL,
				  `first_name` varchar(55) COLLATE utf8mb4_unicode_ci NOT NULL,
				  `last_name` varchar(55) COLLATE utf8mb4_unicode_ci NOT NULL,
				  `product_id` int(11) NOT NULL,
				  `offline` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '',
				  PRIMARY KEY (`ID`),
				  KEY `ORDER` (`order_id`),
				  KEY `PRODUCT` (`product_id`) USING BTREE
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
			);
	}
}