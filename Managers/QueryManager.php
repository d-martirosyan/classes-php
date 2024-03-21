<?php

namespace LevCharity\Managers;

use Automattic\WooCommerce\Utilities\OrderUtil;

class QueryManager
{
    protected \wpdb $wpdb;
    protected static \wpdb $db;
	protected static $product_donors = [];

    public function __construct()
    {
        global $wpdb;
        self::$db = $wpdb;
    }

    public static function getAllDonations(): array
    {
        $query =
            "SELECT DISTINCT
				posts.ID AS ID,
				posts.post_title AS name,
				posts.post_status AS status,
				COALESCE(meta1.meta_value, '') AS currency
			FROM
				" . self::$db->prefix . "posts AS posts
			LEFT JOIN
				" . self::$db->prefix . "postmeta AS meta1
			ON
				posts.ID = meta1.post_id
			AND
				meta1.meta_key = 'base_currency'
			WHERE
				posts.post_type = 'charity-donation'
			AND
				(posts.post_status = 'publish' OR posts.post_status = 'draft')
			ORDER BY ID DESC
			";

        return self::$db->get_results($query);
    }

	public static function getAllDonationsWithProductID(): array
	{
		$query =
			"SELECT DISTINCT
				posts.ID AS ID,
				posts.post_title AS name,
				posts.post_status AS status,
				COALESCE(meta1.meta_value, '') AS product_id
			FROM
				" . self::$db->prefix . "posts AS posts
			LEFT JOIN
				" . self::$db->prefix . "postmeta AS meta1
			ON
				posts.ID = meta1.post_id
			AND
				meta1.meta_key = 'corresponded_product_id'
			WHERE
				posts.post_type = 'charity-donation'
			AND
				(posts.post_status = 'publish' OR posts.post_status = 'draft')
			ORDER BY ID DESC
			";

		return self::$db->get_results($query);
	}

    public static function getCustomerOrders(string $customer_email): array
    {
        if (!$customer_email) {
            return [];
        }
        if (OrderUtil::custom_orders_table_usage_is_enabled()) {
            $query =
                "SELECT DISTINCT
					orders.id AS order_id,
					GROUP_CONCAT(DISTINCT CASE WHEN order_itemmeta.meta_value <> '0' THEN order_itemmeta.meta_value END SEPARATOR ', ') AS parent_id,
					GROUP_CONCAT(DISTINCT CASE WHEN order_itemmeta2.meta_value <> '0' THEN order_itemmeta2.meta_value END SEPARATOR ', ') AS variation_id,
					orders.total_amount AS total,
					orders.status AS status,
					orders.currency AS currency,
					orders.date_created_gmt AS date_created,
					COALESCE(order_itemmeta3.meta_value, '') AS offline
				FROM
					" . self::$db->prefix . "wc_orders AS orders
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_items AS order_items
				ON
					orders.id = order_items.order_id
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta as order_itemmeta
				ON
					order_items.order_item_id = order_itemmeta.order_item_id
				AND
					order_itemmeta.meta_key = '_product_id'
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta2
				ON
					order_items.order_item_id = order_itemmeta2.order_item_id
				AND
					order_itemmeta2.meta_key = '_variation_id'
				LEFT JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta3
				ON
					order_items.order_item_id = order_itemmeta3.order_item_id
				AND
					order_itemmeta3.meta_key = '_is_offline'
				WHERE
					orders.billing_email = %s
				GROUP BY orders.id
				";
        } else {
            $query =
                "SELECT DISTINCT
					stats.order_id AS order_id,
					GROUP_CONCAT(DISTINCT CASE WHEN order_itemmeta.meta_value <> '0' THEN order_itemmeta.meta_value END SEPARATOR ', ') AS parent_id,
					GROUP_CONCAT(DISTINCT CASE WHEN order_itemmeta2.meta_value <> '0' THEN order_itemmeta2.meta_value END SEPARATOR ', ') AS variation_id,
					stats.total_sales AS total,
					stats.status AS status,
					meta2.meta_value AS currency,
					stats.date_created_gmt AS date_created,
					COALESCE(order_itemmeta3.meta_value, '') AS offline
				FROM
					" . self::$db->prefix . "wc_order_stats AS stats
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_items AS order_items
				ON
					stats.order_id = order_items.order_id
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
				ON
					order_items.order_item_id = order_itemmeta.order_item_id
				AND
					order_itemmeta.meta_key = '_product_id'
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta2
				ON
					order_items.order_item_id = order_itemmeta2.order_item_id
				AND
					order_itemmeta2.meta_key = '_variation_id'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta
				ON
					order_items.order_id = meta.post_id
				AND
					meta.meta_key = '_billing_email'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta2
				ON
					order_items.order_id = meta2.post_id
				AND
					meta2.meta_key = '_order_currency'
				LEFT JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta3
				ON
					order_items.order_item_id = order_itemmeta3.order_item_id
				AND
					order_itemmeta3.meta_key = '_is_offline'
				WHERE
					meta.meta_value = %s
				GROUP BY meta.post_id
				";
        }
        return self::$db->get_results(self::$db->prepare($query, $customer_email));
    }

    public static function getCustomerDataByEmail(string $customer_email): array
    {
        if (OrderUtil::custom_orders_table_usage_is_enabled()) {
            $query =
                "SELECT DISTINCT
					COALESCE(addresses.first_name, '') AS firstName,
					COALESCE(addresses.last_name, '') AS lastName
				FROM
					" . self::$db->prefix . "wc_order_addresses AS addresses
				WHERE
					addresses.email = %s
				AND
					addresses.address_type = 'billing'
				";
        } else {
            $query =
                "SELECT DISTINCT
					COALESCE(meta2.meta_value, '') AS firstName,
					COALESCE(meta3.meta_value, '') AS lastName
				FROM
					" . self::$db->prefix . "postmeta AS meta
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta2
				ON
					meta.post_id = meta2.post_id
				AND
					meta2.meta_key = '_billing_first_name'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta3
				ON
					meta.post_id = meta3.post_id
				AND
					meta3.meta_key = '_billing_last_name'
				WHERE
					meta.meta_key = '_billing_email'
				AND
					meta.meta_value = %s
				";
        }
        return self::$db->get_results(self::$db->prepare($query, $customer_email));
    }

    public static function getTopSellersByCurrency(string $currency): array
    {
        if (OrderUtil::custom_orders_table_usage_is_enabled()) {
            $query =
                "SELECT DISTINCT
					order_itemmeta.meta_value AS id,
					order_items.order_item_name AS productName,
					terms.name AS productType,
					ROUND(SUM(CAST(orders.total_amount AS DECIMAL(10,2))), 2) AS totalSales,
					COUNT(DISTINCT orders.id) AS totalOrders
				FROM
					" . self::$db->prefix . "wc_orders AS orders
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_items AS order_items
				ON
					orders.id = order_items.order_id
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
				ON
					order_items.order_item_id = order_itemmeta.order_item_id
				AND
					order_itemmeta.meta_key = '_product_id'
				INNER JOIN
					" . self::$db->prefix . "term_relationships AS term_rel
				ON
					order_itemmeta.meta_value = term_rel.object_id
				INNER JOIN
					" . self::$db->prefix . "term_taxonomy AS term_tax
				ON
					term_rel.term_taxonomy_id = term_tax.term_taxonomy_id
				AND
					term_tax.taxonomy = 'product_type'
				INNER JOIN
					" . self::$db->prefix . "terms AS terms
				ON
					term_tax.term_id = terms.term_id
				AND
					term_tax.taxonomy = 'product_type'
				WHERE
					orders.currency = %s
				AND
					(orders.status = 'wc-processing' OR orders.status = 'wc-completed')
				GROUP BY order_itemmeta.meta_value
				ORDER BY ROUND(SUM(CAST(orders.total_amount AS DECIMAL(10,2))), 2) DESC
				";
        } else {
            $query =
                "SELECT DISTINCT
					order_itemmeta.meta_value AS id,
					order_items.order_item_name AS productName,
					terms.name AS productType,
					ROUND(SUM(CAST(stats.total_sales AS DECIMAL(10,2))), 2) AS totalSales,
					COUNT(DISTINCT stats.order_id) AS totalOrders
				FROM
					" . self::$db->prefix . "wc_order_stats AS stats
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_items AS order_items
				ON
					stats.order_id = order_items.order_id
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
				ON
					order_items.order_item_id = order_itemmeta.order_item_id
				AND
					order_itemmeta.meta_key = '_product_id'
				INNER JOIN
					" . self::$db->prefix . "term_relationships AS term_rel
				ON
					order_itemmeta.meta_value = term_rel.object_id
				INNER JOIN
					" . self::$db->prefix . "term_taxonomy AS term_tax
				ON
					term_rel.term_taxonomy_id = term_tax.term_taxonomy_id
				AND
					term_tax.taxonomy = 'product_type'
				INNER JOIN
					" . self::$db->prefix . "terms AS terms
				ON
					term_tax.term_id = terms.term_id
				AND
					term_tax.taxonomy = 'product_type'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta
				ON
					order_items.order_id = meta.post_id
				AND
					meta.meta_key = '_order_currency'
				WHERE
					meta.meta_value = %s
				AND
					(stats.status = 'wc-processing' OR stats.status = 'wc-completed')
				GROUP BY order_itemmeta.meta_value
				ORDER BY ROUND(SUM(CAST(stats.total_sales AS DECIMAL(10,2))), 2) DESC
				";
        }

        return self::$db->get_results(self::$db->prepare($query, $currency));
    }

    public static function getHighestDonations(int $displayLimit, string $currency): array
    {
        if (OrderUtil::custom_orders_table_usage_is_enabled()) {
            $query =
                "SELECT DISTINCT
    				ROUND(CAST(orders.total_amount AS DECIMAL(10,2)), 2) AS total,
					orders.date_created_gmt AS date_created,
					addresses.first_name AS firstName,
					addresses.last_name AS lastName,
					addresses.email AS email
    			FROM
					" . self::$db->prefix . "wc_orders AS orders
				INNER JOIN
					" . self::$db->prefix . "wc_order_addresses AS addresses
				ON
					orders.id = addresses.order_id
				WHERE
					orders.currency = %s
				AND
					(orders.status = 'wc-processing' OR orders.status = 'wc-completed')
				AND
					addresses.address_type = 'billing'
				ORDER BY
        			ROUND(CAST(orders.total_amount AS DECIMAL(10,2)), 2) DESC
    			LIMIT %d
				";
        } else {
            $query =
                "SELECT
					ROUND(CAST(stats.total_sales AS DECIMAL(10,2)), 2) AS total,
					stats.date_created_gmt AS date_created,
					meta2.meta_value AS firstName,
					meta3.meta_value AS lastName,
					meta4.meta_value AS email
				FROM
					" . self::$db->prefix . "wc_order_stats AS stats
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta
				ON
					stats.order_id = meta.post_id
				AND
					meta.meta_key = '_order_currency'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta2
				ON
					stats.order_id = meta2.post_id
				AND
					meta2.meta_key = '_billing_first_name'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta3
				ON
					stats.order_id = meta3.post_id
				AND
					meta3.meta_key = '_billing_last_name'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta4
				ON
					stats.order_id = meta4.post_id
				AND
					meta4.meta_key = '_billing_email'
				WHERE
					meta.meta_value = %s
				ORDER BY
					ROUND(CAST(stats.total_sales AS DECIMAL(10,2)), 2) DESC
				LIMIT %d";
        }

        return self::$db->get_results(self::$db->prepare($query, $currency, $displayLimit));
    }

    public static function getHighestDonors(int $displayLimit, string $currency): array
    {
        if (OrderUtil::custom_orders_table_usage_is_enabled()) {
            $query =
                "SELECT DISTINCT
					ROUND(SUM(CAST(orders.total_amount AS DECIMAL(10,2))), 2) AS total,
					COUNT(DISTINCT orders.id) AS totalOrders,
					addresses.email AS email,
					addresses.first_name AS firstName,
					addresses.last_name AS lastName
				FROM
					" . self::$db->prefix . "wc_orders AS orders
				INNER JOIN
					" . self::$db->prefix . "wc_order_addresses AS addresses
				ON
					orders.id = addresses.order_id
				WHERE
					orders.currency = %s
				AND
					(orders.status = 'wc-processing' OR orders.status = 'wc-completed')
				AND
					addresses.address_type = 'billing'
				GROUP BY
					addresses.email
				ORDER BY
					ROUND(SUM(CAST(orders.total_amount AS DECIMAL(10,2))), 2) DESC
				LIMIT %d;
					";
        } else {
            $query =
                "SELECT DISTINCT
					ROUND(SUM(CAST(stats.total_sales AS DECIMAL(10,2))), 2) AS total,
					COUNT(DISTINCT stats.order_id) AS totalOrders,
					meta4.meta_value AS email,
					meta2.meta_value AS firstName,
					meta3.meta_value AS lastName
				FROM
					" . self::$db->prefix . "wc_order_stats AS stats
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta
				ON
					stats.order_id = meta.post_id
				AND
					meta.meta_key = '_order_currency'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta2
				ON
					stats.order_id = meta2.post_id
				AND
					meta2.meta_key = '_billing_first_name'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta3
				ON
					stats.order_id = meta3.post_id
				AND
					meta3.meta_key = '_billing_last_name'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta4
				ON
					stats.order_id = meta4.post_id
				AND
					meta4.meta_key = '_billing_email'
				WHERE
					meta.meta_value = %s
				AND
					(stats.status = 'wc-processing' OR stats.status = 'wc-completed')
				GROUP BY
					meta4.meta_value
				ORDER BY
					ROUND(SUM(CAST(stats.total_sales AS DECIMAL(10,2))), 2) DESC
				LIMIT %d;
					";
        }

        return self::$db->get_results(self::$db->prepare($query, $currency, $displayLimit));
    }

    public static function getDonationShortcodeUsedNumber(string $shortcode): array
    {
        $like = '%' . self::$db->esc_like($shortcode) . '%';

        $direct_query =
            "SELECT
    			COUNT(*) AS count,
    			COALESCE(GROUP_CONCAT(posts.ID SEPARATOR ','), '') AS post_ids
			FROM
			    " . self::$db->prefix . "posts AS posts
			WHERE
				posts.post_content LIKE %s
			AND
				posts.post_status = 'publish'
			";

        $direct_search = self::$db->get_results(self::$db->prepare($direct_query, $like));

        $translation_query =
            "SELECT
			COUNT(*) AS count
		FROM
			" . self::$db->prefix . "icl_string_translations AS translations
		WHERE
			translations.value = %s
		";

        $translation_search = self::$db->get_results(self::$db->prepare($translation_query, $shortcode));

        if ($direct_search[0]->count === '0') {
            $translation_search[0]->post_ids = '';
            return $translation_search;
        }

        return $direct_search;
    }

    public static function getProductIdsByOrderProductId(int $product_id): array
    {
        $query =
            "SELECT DISTINCT
				order_meta1.meta_value AS product_id,
				order_meta2.meta_value AS variation_id
            FROM
                " . self::$db->prefix . "woocommerce_order_itemmeta AS order_meta1
			INNER JOIN
                " . self::$db->prefix . "woocommerce_order_itemmeta AS order_meta2
            ON
            	order_meta1.order_item_id = order_meta2.order_item_id
            WHERE
                order_meta1.order_item_id = %d
            AND
            	order_meta1.meta_key = '_product_id'
            AND
            	order_meta2.meta_key = '_variation_id'
           ";
        return self::$db->get_results(self::$db->prepare($query, $product_id));
    }

    public static function getOrderData(string $start_date = '', string $end_date = '', string $currency = '', string $onlyOffline = ''): array
    {
        $where_clauses = [];
        $prepare_parameters = [];

        if ($start_date && $end_date) {
            $startDateTime = new \DateTime($start_date);
            $endDateTime = new \DateTime($end_date);
            $formatted_start_date = $startDateTime->format('Y-m-d H:i:s');
            $formatted_end_date = $endDateTime->format('Y-m-d H:i:s');

            if (OrderUtil::custom_orders_table_usage_is_enabled()) {
                $where_clauses[] = "orders.date_created_gmt BETWEEN %s AND %s";
            } else {
                $where_clauses[] = "stats.date_created_gmt BETWEEN %s AND %s";
            }

            $prepare_parameters[] = $formatted_start_date;
            $prepare_parameters[] = $formatted_end_date;
        }

        if ($currency) {
            if (OrderUtil::custom_orders_table_usage_is_enabled()) {
                $where_clauses[] = "orders.currency = %s";
            } else {
                $where_clauses[] = "meta4.meta_value = %s";
            }

            $prepare_parameters[] = $currency;
        }

        if ($onlyOffline) {
            $where_clauses[] = "order_itemmeta2.meta_value = %s";
            $prepare_parameters[] = $onlyOffline;
        }

        $imploded_where_clauses = "";
        if (!empty($where_clauses)) {
            $imploded_where_clauses = " AND " . implode(" AND ", $where_clauses);
        }

        if (OrderUtil::custom_orders_table_usage_is_enabled()) {
            $query =
                "SELECT DISTINCT
				orders.id AS ID,
				GROUP_CONCAT(DISTINCT order_items.order_item_name SEPARATOR ', ') AS itemName,
				COALESCE(addresses.first_name, '') AS firstName,
				COALESCE(addresses.last_name, '') AS lastName,
				addresses.email AS email,
				orders.currency AS currency,
				COALESCE(order_itemmeta2.meta_value, '') AS offline,
				orders.total_amount AS total,
				orders.date_created_gmt AS date,
				orders.status AS status
    			FROM
					" . self::$db->prefix . "wc_orders AS orders
				INNER JOIN
					" . self::$db->prefix . "wc_order_addresses AS addresses
				ON
					orders.id = addresses.order_id
				AND
					addresses.address_type = 'billing'
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_items AS order_items
				ON
					orders.id = order_items.order_id
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
				ON
					order_items.order_item_id = order_itemmeta.order_item_id
				LEFT JOIN
                	" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta2
				ON
					order_items.order_item_id = order_itemmeta2.order_item_id
				AND
					order_itemmeta2.meta_key = '_is_offline'
				WHERE
					1 = 1" . $imploded_where_clauses . "
				GROUP BY
					orders.id
				ORDER BY
					orders.date_created_gmt DESC
				";
        } else {
            $query =
                "SELECT DISTINCT
					order_items.order_id AS ID,
					GROUP_CONCAT(DISTINCT order_items.order_item_name SEPARATOR ', ') AS itemName,
					COALESCE(meta.meta_value, '') AS firstName,
					COALESCE(meta2.meta_value, '') AS lastName,
					meta3.meta_value AS email,
					COALESCE(meta4.meta_value, '') AS currency,
					COALESCE(meta5.meta_value, '') AS total,
					COALESCE(order_itemmeta2.meta_value, '') AS offline,
					stats.date_created_gmt AS date,
					stats.status AS status
				FROM
					" . self::$db->prefix . "woocommerce_order_items AS order_items
				INNER JOIN
					" . self::$db->prefix . "wc_order_stats AS stats
				ON
					order_items.order_id = stats.order_id
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta
				ON
					stats.order_id = meta.post_id
				AND
					meta.meta_key = '_billing_first_name'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta2
				ON
					stats.order_id = meta2.post_id
				AND
					meta2.meta_key = '_billing_last_name'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta3
				ON
					stats.order_id = meta3.post_id
				AND
					meta3.meta_key = '_billing_email'
				LEFT JOIN
					" . self::$db->prefix . "postmeta AS meta4
				ON
					order_items.order_id = meta4.post_id
				AND
					meta4.meta_key = '_order_currency'
				LEFT JOIN
					" . self::$db->prefix . "postmeta AS meta5
				ON
					order_items.order_id = meta5.post_id
				AND
					meta5.meta_key = '_order_total'
				LEFT JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
				ON
					order_items.order_item_id = order_itemmeta.order_item_id
				AND
					order_itemmeta.meta_key = '_user_paying_currency'
				LEFT JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta2
				ON
					order_items.order_item_id = order_itemmeta2.order_item_id
				AND
					order_itemmeta2.meta_key = '_is_offline'
				WHERE
					1 = 1" . $imploded_where_clauses . "
				GROUP BY
					order_items.order_id
				ORDER BY
					stats.date_created_gmt DESC";
        }
        return self::$db->get_results(self::$db->prepare($query, ...$prepare_parameters));
    }

    public static function getOfflineOrders()
    {
	    $query =
		    "SELECT DISTINCT
				donations.order_id as ID,
				product.post_title as title,
				donations.first_name as firstName,
				donations.last_name as lastName,
				donations.billing_email as email,
				donations.total as total,
				donations.currency as currency,
				donations.order_date as date,
				term.name AS product_type,
				donations.order_status as status
            FROM
				" . self::$db->prefix . "levcharity_donations AS donations
			INNER JOIN
				" . self::$db->prefix . "posts AS product
			ON
				donations.product_id = product.ID
			INNER JOIN
				" . self::$db->prefix . "term_taxonomy AS term_taxonomy
			JOIN
				" . self::$db->prefix . "terms AS term
			ON
				term_taxonomy.term_id = term.term_id
			JOIN
				" . self::$db->prefix . "term_relationships AS term_relationships
			ON
				term_taxonomy.term_taxonomy_id = term_relationships.term_taxonomy_id
			WHERE
				donations.offline = 'yes'
			AND
				term_relationships.object_id = donations.product_id
			AND
				term_taxonomy.taxonomy = 'product_type'
			";

	    return self::$db->get_results(self::$db->prepare($query));
    }

	public static function getReportingDonationOrders(): array
	{

		if (OrderUtil::custom_orders_table_usage_is_enabled()) {
			$query =
				"SELECT DISTINCT
				orders.id AS ID,
				GROUP_CONCAT(DISTINCT order_items.order_item_name SEPARATOR ', ') AS itemName,
				product.post_title AS title,
				COALESCE(addresses.first_name, '') AS firstName,
				COALESCE(addresses.last_name, '') AS lastName,
				addresses.email AS email,
				orders.currency AS currency,
				COALESCE(order_itemmeta2.meta_value, '') AS offline,
				orders.total_amount AS total,
				orders.date_created_gmt AS date,
				orders.status AS status
    			FROM
					" . self::$db->prefix . "wc_orders AS orders
				INNER JOIN
					" . self::$db->prefix . "wc_order_addresses AS addresses
				ON
					orders.id = addresses.order_id
				AND
					addresses.address_type = 'billing'
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_items AS order_items
				ON
					orders.id = order_items.order_id
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta3
				ON
					order_items.order_item_id = order_itemmeta3.order_item_id
				AND
					order_itemmeta3.meta_key = '_product_id'
				LEFT JOIN
					" . self::$db->prefix . "posts AS product
				ON
					order_itemmeta3.meta_value = product.ID
				INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
				ON
					order_items.order_item_id = order_itemmeta.order_item_id
				LEFT JOIN
                	" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta2
				ON
					order_items.order_item_id = order_itemmeta2.order_item_id
				AND
					order_itemmeta2.meta_key = '_is_offline'
				GROUP BY
					orders.id
				ORDER BY
					orders.date_created_gmt DESC
				";
		} else {
			$query =
				"SELECT DISTINCT
					order_items.order_id AS ID,
					GROUP_CONCAT(DISTINCT order_items.order_item_name SEPARATOR ', ') AS itemName,
					product.post_title AS title,
					COALESCE(meta.meta_value, '') AS firstName,
					COALESCE(meta2.meta_value, '') AS lastName,
					meta3.meta_value AS email,
					COALESCE(meta4.meta_value, '') AS currency,
					COALESCE(meta5.meta_value, '') AS total,
					COALESCE(order_itemmeta2.meta_value, '') AS offline,
					stats.date_created_gmt AS date,
					stats.status AS status
				FROM
					" . self::$db->prefix . "woocommerce_order_items AS order_items
				INNER JOIN
					" . self::$db->prefix . "wc_order_stats AS stats
				ON
					order_items.order_id = stats.order_id
			   INNER JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta3
				ON
					order_items.order_item_id = order_itemmeta3.order_item_id
				AND
					order_itemmeta3.meta_key = '_product_id'
				LEFT JOIN
					" . self::$db->prefix . "posts AS product
				ON
					order_itemmeta3.meta_value = product.ID
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta
				ON
					stats.order_id = meta.post_id
				AND
					meta.meta_key = '_billing_first_name'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta2
				ON
					stats.order_id = meta2.post_id
				AND
					meta2.meta_key = '_billing_last_name'
				INNER JOIN
					" . self::$db->prefix . "postmeta AS meta3
				ON
					stats.order_id = meta3.post_id
				AND
					meta3.meta_key = '_billing_email'
				LEFT JOIN
					" . self::$db->prefix . "postmeta AS meta4
				ON
					order_items.order_id = meta4.post_id
				AND
					meta4.meta_key = '_order_currency'
				LEFT JOIN
					" . self::$db->prefix . "postmeta AS meta5
				ON
					order_items.order_id = meta5.post_id
				AND
					meta5.meta_key = '_order_total'
				LEFT JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
				ON
					order_items.order_item_id = order_itemmeta.order_item_id
				AND
					order_itemmeta.meta_key = '_user_paying_currency'
				LEFT JOIN
					" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta2
				ON
					order_items.order_item_id = order_itemmeta2.order_item_id
				AND
					order_itemmeta2.meta_key = '_is_offline'
				GROUP BY
					order_items.order_id
				ORDER BY
					stats.date_created_gmt DESC";
		}
		return self::$db->get_results(self::$db->prepare($query));
	}

	public static function getReportingDonationOrders2(): array
	{
		$query =
			"SELECT DISTINCT
				donations.order_id as ID,
				product.post_title as title,
				donations.first_name as firstName,
				donations.last_name as lastName,
				donations.billing_email as email,
				donations.total as total,
				donations.currency as currency,
				donations.order_date as date,
				donations.order_status as status
            FROM
				" . self::$db->prefix . "levcharity_donations AS donations
			INNER JOIN
				" . self::$db->prefix . "posts AS product
			ON
				donations.product_id = product.ID
			ORDER BY
				donations.order_date DESC
			";

		return self::$db->get_results(self::$db->prepare($query));
	}

    public static function getAllDonors(): array
    {
        if (OrderUtil::custom_orders_table_usage_is_enabled()) {
            $query =
                "SELECT DISTINCT
					GROUP_CONCAT(DISTINCT
						CASE
							WHEN addresses.first_name <> 'Anonymous'
								THEN COALESCE(addresses.first_name, '')
								ELSE NULL
						END SEPARATOR ',') AS firstName,
					GROUP_CONCAT(DISTINCT COALESCE(addresses.last_name, NULL) SEPARATOR ',') AS lastName,
					COALESCE(addresses.email, '') AS email,
					CASE
						WHEN COUNT(DISTINCT orders.currency) = 1 THEN
							orders.currency
						ELSE
							GROUP_CONCAT(orders.currency SEPARATOR ',')
						END AS currency,
					CASE
						WHEN COUNT(DISTINCT orders.currency) = 1 THEN
							ROUND(SUM(CAST(orders.total_amount AS DECIMAL(10,2))), 2)
						ELSE
							GROUP_CONCAT(orders.total_amount SEPARATOR ',')
					END AS total,
					COUNT(DISTINCT orders.id) AS totalOrders
					FROM
						" . self::$db->prefix . "wc_order_addresses AS addresses
					INNER JOIN
						" . self::$db->prefix . "wc_orders AS orders
					ON
						addresses.order_id = orders.id
					AND
						(orders.status = 'wc-processing' OR orders.status = 'wc-completed')
					WHERE
						addresses.address_type = 'billing'
					GROUP BY
						addresses.email
					ORDER BY
						addresses.last_name ASC
					";
        } else {
            $query =
                "SELECT DISTINCT
					GROUP_CONCAT(DISTINCT
						CASE
							WHEN meta2.meta_value <> 'Anonymous'
								THEN COALESCE(meta2.meta_value, '')
								ELSE NULL
						END SEPARATOR ',') AS firstName,
					GROUP_CONCAT(DISTINCT COALESCE(meta3.meta_value, NULL) SEPARATOR ',') AS lastName,
					COALESCE(meta.meta_value, '') AS email,
					CASE
						WHEN COUNT(DISTINCT meta4.meta_value) = 1 THEN
							meta4.meta_value
						ELSE
							GROUP_CONCAT(meta4.meta_value SEPARATOR ',')
						END AS currency,
					CASE
						WHEN COUNT(DISTINCT meta4.meta_value) = 1 THEN
							ROUND(SUM(CAST(stats.total_sales AS DECIMAL(10,2))), 2)
						ELSE
							GROUP_CONCAT(stats.total_sales SEPARATOR ',')
					END AS total,
					COUNT(DISTINCT stats.order_id) AS totalOrders
					FROM
						" . self::$db->prefix . "wc_order_stats AS stats
					INNER JOIN
						" . self::$db->prefix . "postmeta AS meta
					ON
						stats.order_id = meta.post_id
					AND
						meta.meta_key = '_billing_email'
					LEFT JOIN
						" . self::$db->prefix . "postmeta AS meta2
					ON
						stats.order_id = meta2.post_id
					AND
						meta2.meta_key = '_billing_first_name'
					LEFT JOIN
						" . self::$db->prefix . "postmeta AS meta3
					ON
						stats.order_id = meta3.post_id
					AND
						meta3.meta_key = '_billing_last_name'
					INNER JOIN
						" . self::$db->prefix . "postmeta AS meta4
					ON
						stats.order_id = meta4.post_id
					AND
						meta4.meta_key = '_order_currency'
					WHERE
						(stats.status = 'wc-processing' OR stats.status = 'wc-completed')
					GROUP BY
						meta.meta_value
					ORDER BY
						meta3.meta_value ASC
					";
        }
        return self::$db->get_results($query);
    }


    public static function getSubscriptions(): array
    {
        if (OrderUtil::custom_orders_table_usage_is_enabled()) {
            $query =
                "SELECT DISTINCT
				orders.id AS id,
				orders.parent_order_id AS order_id,
				orders.status AS status,
				orders_meta.meta_value AS start_date,
				orders_meta2.meta_value AS end_date,
				orders_meta3.meta_value AS next_payment
            FROM
                " . self::$db->prefix . "wc_orders AS orders
            LEFT JOIN
				" . self::$db->prefix . "wc_orders_meta AS orders_meta
			ON
				orders.id = orders_meta.order_id
			AND
				orders_meta.meta_key = '_schedule_start'
			LEFT JOIN
				" . self::$db->prefix . "wc_orders_meta AS orders_meta2
			ON
				orders.id = orders_meta2.order_id
			AND
				orders_meta2.meta_key = '_schedule_end'
			LEFT JOIN
				" . self::$db->prefix . "wc_orders_meta AS orders_meta3
			ON
				orders.id = orders_meta3.order_id
			AND
				orders_meta3.meta_key = '_schedule_next_payment'
			WHERE
				orders.type = 'shop_subscription'
           ";
        } else {
            $query =
                "SELECT DISTINCT
				posts.ID AS id,
				posts.post_parent AS order_id,
				posts.post_status AS status,
				meta1.meta_value AS start_date,
				meta2.meta_value AS end_date,
				meta3.meta_value AS next_payment
            FROM
                " . self::$db->prefix . "posts AS posts
			LEFT JOIN
                " . self::$db->prefix . "postmeta AS meta1
            ON
            	posts.ID = meta1.post_id
            AND
            	meta1.meta_key = '_schedule_start'
			LEFT JOIN
                " . self::$db->prefix . "postmeta AS meta2
            ON
            	posts.ID = meta2.post_id
            AND
            	meta2.meta_key = '_schedule_end'
			LEFT JOIN
                " . self::$db->prefix . "postmeta AS meta3
            ON
            	posts.ID = meta3.post_id
            AND
            	meta3.meta_key = '_schedule_next_payment'
            WHERE
                posts.post_type = 'shop_subscription'
            AND
            	posts.post_status != 'auto-draft'
           ";
        }
        return self::$db->get_results($query);
    }

    public static function getCustomerIdByEmail(string $email): int
    {
        if (OrderUtil::custom_orders_table_usage_is_enabled()) {
            $query =
                "SELECT DISTINCT
					orders.customer_id
				FROM
					" . self::$db->prefix . "wc_orders as orders
				WHERE
					orders.billing_email = %s
			   ";
        } else {
            $query =
                "SELECT DISTINCT
					customer.customer_id
				FROM
					" . self::$db->prefix . "wc_customer_lookup as customer
				WHERE
					customer.email = %s
			   ";
        }
        return (int) self::$db->get_var(self::$db->prepare($query, $email));
    }

    public static function sortProductsIdsByPrice(array $ids, string $order_by = 'ASC'): array
    {
        $placeholders = implode(',', array_fill(0, count($ids), '%d'));

        $query = "
			SELECT
				product_meta.product_id AS id,
				product_meta.min_price AS price
			FROM
				" . self::$db->prefix . "wc_product_meta_lookup as product_meta
			WHERE
				product_meta.product_id IN ($placeholders)
			ORDER BY
				CAST(product_meta.min_price AS DECIMAL(10,2))
		";

        if ($order_by === 'ASC' || $order_by === 'DESC') {
            $query .= " " . $order_by;
        }

        $results = self::$db->get_results(self::$db->prepare($query, $ids), ARRAY_A);
        return array_column($results, 'id');
    }

	//This can be used to calculate total and set as product meta
	public static function getProductTotalSalesWithConversion(int $product_id): float
	{
		if (OrderUtil::custom_orders_table_usage_is_enabled()) {
			$query = "SELECT
						SUM(
							CASE
								WHEN
									order_itemmeta2.meta_value = 1 OR order_itemmeta2.meta_value = ''
								THEN
									CAST(orders.total_amount AS DECIMAL(10, 2))
								ELSE
								    CAST(orders.total_amount AS DECIMAL(10, 2)) / CAST(order_itemmeta2.meta_value AS DECIMAL(10, 2))
							END
						)
					FROM
						" . self::$db->prefix . "wc_orders AS orders
					INNER JOIN
						" . self::$db->prefix . "woocommerce_order_items AS order_items
					ON
						orders.id = order_items.order_id
					INNER JOIN
						" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
					ON
						order_items.order_item_id = order_itemmeta.order_item_id
					AND
						(order_itemmeta.meta_key = '_product_id' OR order_itemmeta.meta_key = '_variation_id')
					INNER JOIN
						" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta2
					ON
						order_items.order_item_id = order_itemmeta2.order_item_id
					AND
						order_itemmeta2.meta_key = '_currency_conversion'
					WHERE
						(orders.status = 'wc-processing' OR orders.status = 'wc-completed')
					AND
						order_itemmeta.meta_value = %d
			";
		} else {
			$query = "SELECT
						SUM(
							CASE
								WHEN
									order_itemmeta2.meta_value = 1 OR order_itemmeta2.meta_value = ''
								THEN
									CAST(stats.total_sales AS DECIMAL(10, 2))
								ELSE
								    CAST(stats.total_sales AS DECIMAL(10, 2)) / CAST(order_itemmeta2.meta_value AS DECIMAL(10, 2))
							END
						)
					FROM
						" . self::$db->prefix . "wc_order_stats AS stats
					INNER JOIN
						" . self::$db->prefix . "woocommerce_order_items AS order_items
					ON
						stats.order_id = order_items.order_id
					INNER JOIN
						" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
					ON
						order_items.order_item_id = order_itemmeta.order_item_id
					AND
						(order_itemmeta.meta_key = '_product_id' OR order_itemmeta.meta_key = '_variation_id')
					INNER JOIN
						" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta2
					ON
						order_items.order_item_id = order_itemmeta2.order_item_id
					AND
						order_itemmeta2.meta_key = '_currency_conversion'
					WHERE
						(stats.status = 'wc-processing' OR stats.status = 'wc-completed')
					AND
						order_itemmeta.meta_value = %d
				";
		}
		return (float) self::$db->get_var(self::$db->prepare($query, $product_id));
	}

	public static function getProductTotalSalesWithConversion2(int $product_id): float
	{
		if(!$product_id){
			return 0;
		}

		$query = "
			SELECT
				SUM(
		        CASE
		            WHEN conversion_rate IS NULL OR conversion_rate = 1 OR conversion_rate = 0 THEN total
		            ELSE total / conversion_rate
		        END
		    ) AS calculated_sum
			FROM
				" . self::$db->prefix . "levcharity_donations
			WHERE product_id = %d
			AND order_status IN ('wc-processing', 'wc-completed')
			GROUP BY product_id
		";

		return self::$db->get_var(self::$db->prepare($query, $product_id)) ? (float) self::$db->get_var(self::$db->prepare($query, $product_id)) : 0;
	}

	public static function getOrdersByProductId(int $product_id):array
	{
		if(!$product_id){
			return [];
		}

		if (!self::campaignHasDonors($product_id)) {
			return [];
		}

		$query = "
			SELECT *
			FROM " . self::$db->prefix . "levcharity_donations
			WHERE product_id = %d
			AND order_status IN ('wc-processing', 'wc-completed')
			ORDER BY order_date DESC
		";

		return self::$db->get_results(self::$db->prepare($query, $product_id));
	}

	public static function getP2PCampaignOrdersId(int $product_id):array
	{
		if(!$product_id){
			return [];
		}

		if (!self::campaignHasDonors($product_id)) {
			return [];
		}

		$query = "
			SELECT 
				donations.order_id as order_id,
				donations.first_name as first_name,
				donations.last_name as last_name,
				donations.order_date as order_date,
				donations.currency as currency,
				donations.billing_email as billing_email,
				donations.total as total,
				donations.total as total,
				donations.order_status as order_status,
				posts.post_excerpt as campaign_message,
				meta.meta_value as hide_donor_name,
				meta2.meta_value as hide_donor_amount
			FROM " . self::$db->prefix . "levcharity_donations as donations
			LEFT JOIN
				" . self::$db->prefix . "postmeta as meta
			ON
				donations.order_id = meta.post_id
			AND 
				meta.meta_key = 'hide_donor_name'
			LEFT JOIN
				" . self::$db->prefix . "postmeta as meta2
			ON
				donations.order_id = meta2.post_id
			AND 
				meta2.meta_key = 'hide_donor_amount'
			LEFT JOIN
				" . self::$db->prefix . "posts as posts
			ON
				donations.order_id = posts.ID
			WHERE product_id = %d
			ORDER BY order_date DESC
		";

		return self::$db->get_results(self::$db->prepare($query, $product_id));
	}

    public static function getOrdersForDatatableByProductID(int $product_id)
    {
    	if(!$product_id){
    		return [];
	    }

	    $query =
		    "SELECT DISTINCT
    				stats.order_id AS order_id,
					stats.date_created_gmt AS order_date,
					COALESCE(order_itemmeta7.meta_value, '') AS currency,
					stats.total_sales AS total,
					stats.status AS order_status,
					COALESCE(order_itemmeta.meta_value, '') AS conversion_rate,
					COALESCE(order_itemmeta2.meta_value, '') AS display_name,
					COALESCE(order_itemmeta3.meta_value, '') AS campaign_message,
					order_itemmeta4.meta_value AS quantity,
					COALESCE(order_itemmeta5.meta_value, '') AS anonymous,
					postmeta.meta_value AS billing_email,
					postmeta2.meta_value AS first_name,
					postmeta3.meta_value AS last_name
				FROM
					" . self::$db->prefix . "wc_order_stats AS stats
				INNER JOIN
                	" . self::$db->prefix . "woocommerce_order_items AS order_items
            	ON
                	stats.order_id = order_items.order_id
                LEFT JOIN
                	" . self::$db->prefix . "postmeta AS postmeta
            	ON
                	stats.order_id = postmeta.post_id
                AND
                	postmeta.meta_key = '_billing_email'
                LEFT JOIN
                " . self::$db->prefix . "postmeta AS postmeta2
            	ON
                	stats.order_id = postmeta2.post_id
                AND
                	postmeta2.meta_key = '_billing_first_name'
                LEFT JOIN
                " . self::$db->prefix . "postmeta AS postmeta3
            	ON
                	stats.order_id = postmeta3.post_id
                AND
                	postmeta3.meta_key = '_billing_last_name'
                LEFT JOIN
                	" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta
            	ON
                	order_items.order_item_id = order_itemmeta.order_item_id
                AND
                	order_itemmeta.meta_key = '_currency_conversion'
                LEFT JOIN
                	" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta2
            	ON
                	order_items.order_item_id = order_itemmeta2.order_item_id
                AND
                	order_itemmeta2.meta_key = '_display_name'
                LEFT JOIN
                	" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta3
            	ON
                	order_items.order_item_id = order_itemmeta3.order_item_id
                AND
                	order_itemmeta3.meta_key = '_campaign_message'
                LEFT JOIN
                	" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta4
            	ON
                	order_items.order_item_id = order_itemmeta4.order_item_id
                AND
                	order_itemmeta4.meta_key = '_qty'
            	LEFT JOIN
                	" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta5
            	ON
                	order_items.order_item_id = order_itemmeta5.order_item_id
                AND
                	order_itemmeta5.meta_key = '_anonymous_donation'
                INNER JOIN
                	" . self::$db->prefix . "woocommerce_order_itemmeta AS order_itemmeta6
            	ON
                	order_items.order_item_id = order_itemmeta6.order_item_id
                LEFT OUTER JOIN
                " . self::$db->prefix . "woocommerce_order_itemmeta as order_itemmeta7
				ON
					order_items.order_item_id = order_itemmeta7.order_item_id
				AND
					order_itemmeta7.meta_key = '_user_paying_currency'
                WHERE
                	(order_itemmeta6.meta_key = '_product_id' OR order_itemmeta6.meta_key = '_variation_id')
                AND
                	order_itemmeta6.meta_value = %d
        		ORDER BY stats.date_created_gmt DESC
				";

		return self::$db->get_results(self::$db->prepare($query, $product_id));
    }


	public static function campaignHasDonors(int $product_id): bool
	{
		$query =
			"SELECT
				order_id
            FROM
                " . self::$db->prefix . "wc_order_product_lookup
            WHERE
				product_id = %d
            LIMIT
                1
            ";
		$order_id = self::$db->get_var(self::$db->prepare($query, $product_id));
		return $order_id && $order_id > 0;
	}

    public static function isOfflineOrder(int $order_id): string
    {
        $query =
            "SELECT DISTINCT
				COALESCE(order_itemmeta.meta_value, '')
            FROM
                " . self::$db->prefix . "woocommerce_order_items as order_items
            LEFT JOIN
                " . self::$db->prefix . "woocommerce_order_itemmeta as order_itemmeta
            ON
                order_items.order_item_id = order_itemmeta.order_item_id
            AND
                order_itemmeta.meta_key = '_is_offline'
            WHERE
				order_items.order_id = %d
            ";

        return self::$db->get_var(self::$db->prepare($query, $order_id)) ?? '';
    }

    public static function getAllProductsByType(string $product_type_slug, $includeDraft = false): array
    {
        $query =
            "SELECT DISTINCT
				posts.ID as ID,
				posts.post_title as name,
				posts.post_status as status
            FROM
                " . self::$db->prefix . "posts as posts
            INNER JOIN
                " . self::$db->prefix . "term_relationships as term_rel
            ON
                posts.ID = term_rel.object_id
			INNER JOIN
                " . self::$db->prefix . "terms as terms
            ON
                term_rel.term_taxonomy_id = terms.term_id
			AND
                terms.slug = %s
            INNER JOIN
                " . self::$db->prefix . "term_taxonomy as term_tax
            ON
                term_rel.term_taxonomy_id = term_tax.term_id
            WHERE
                posts.post_type = 'product'
			AND
                posts.post_status = 'publish'";

        if ($includeDraft) {
            $query .= " OR posts.post_status = 'draft'";
        }

        $query .= " AND term_tax.taxonomy = 'product_type'";

        return self::$db->get_results(self::$db->prepare($query, $product_type_slug));
    }

    public static function getCurrencyId(string $currency): int
    {
        $query =
            "SELECT DISTINCT
				COALESCE(currencies.id, 0)
            FROM
                " . self::$db->prefix . "levcharity_currencies as currencies
            WHERE
                currencies.currency = %s
			";

        return (int) self::$db->get_var(self::$db->prepare($query, $currency));
    }

    public static function getCurrencyDate(string $currency): string
    {
        $query =
            "SELECT DISTINCT
				COALESCE(currencies.date, '')
            FROM
                " . self::$db->prefix . "levcharity_currencies as currencies
            WHERE
                currencies.currency = %s
			";

        return self::$db->get_var(self::$db->prepare($query, $currency));
    }

    public static function getCurrencyConversion(string $currency, string $specific_rate = ''): int|float|array
    {
        $query =
            "SELECT DISTINCT
				COALESCE(currencies.rates, '')
            FROM
                " . self::$db->prefix . "levcharity_currencies as currencies
            WHERE
                currencies.currency = %s
			";

        $result = self::$db->get_var(self::$db->prepare($query, $currency));

        if (empty($result) && '' !== $specific_rate) {
            return [];
        }

        if (empty($result) && '' === $specific_rate) {
            return 0;
        }

        $unserialized_result = unserialize($result, ['allowed_classes' => false]);

        if ('' === $specific_rate) {
            return $unserialized_result;
        }

        return $unserialized_result[$specific_rate] ?? 0;
    }

	public static function getAllCurrencyConversions(): array {
		$query =
			"SELECT DISTINCT
				currencies.currency AS currency,
				currencies.rates AS rates,
				currencies.date AS date
            FROM
                " . self::$db->prefix . "levcharity_currencies as currencies
			";
		return self::$db->get_results($query);
	}

}
