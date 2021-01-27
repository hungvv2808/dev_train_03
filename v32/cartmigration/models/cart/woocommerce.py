import copy
import csv
from urllib.parse import unquote

import chardet

from v32.cartmigration.libs.utils import *
from v32.cartmigration.models.cart.wordpress import LeCartWordpress


# tested with woocommerce335
class LeCartWoocommerce(LeCartWordpress):
    WARNING_VARIANT_LIMIT = 100

    def __init__(self, data=None):
        super().__init__(data)
        self.product_types = dict()
        self.is_variant_limit = False

    def display_config_source(self):
        parent = super().display_config_source()
        url_query = self.get_connector_url('query')
        self._notice['src']['language_default'] = 1
        self._notice['src']['category_root'] = 1
        storage_cat_data = dict()
        storage_cat_data[self._notice['src']['language_default']] = 0
        self._notice['src']['store_category'] = storage_cat_data
        self._notice['src']['support']['site_map'] = False
        self._notice['src']['support']['category_map'] = False
        self._notice['src']['support']['attribute_map'] = False
        self._notice['src']['support']['wpml'] = False
        self._notice['src']['support']['yoast_seo'] = False
        self._notice['src']['support']['manufacturers'] = False
        self._notice['src']['support']['product_bundle'] = False
        self._notice['src']['support']['customer_point_rewards'] = False
        self._notice['src']['support']['addons'] = False
        self._notice['src']['support']['plugin_pre_ord'] = False
        self._notice['src']['support']['plugin_order_status'] = False
        self._notice['src']['support']['custom_order_status'] = False
        query_active_plugins = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_options WHERE option_name = 'active_plugins'"
        }
        active_plugins = self.get_connector_data(self.get_connector_url('query'),
                                                 {'query': json.dumps(query_active_plugins)})
        active_langs = list()
        if active_plugins and active_plugins['data']:
            active_plugin = active_plugins['data'][0]
            active_plugin_v = active_plugin['option_value']
            if active_plugin_v:
                active_plugin_v_data = php_unserialize(active_plugin_v)
                if active_plugin_v_data and isinstance(active_plugin_v_data, dict):
                    active_plugin_v_data = list(active_plugin_v_data.values())
                if active_plugin_v_data:
                    if "woocommerce-multilingual/wpml-woocommerce.php" in active_plugin_v_data:
                        self._notice['src']['support']['wpml'] = True
                        query_active_languages = {
                            'type': 'select',
                            'query': "SELECT * FROM `_DBPRF_options` WHERE `option_name` = 'icl_sitepress_settings'"
                        }
                        options_data = self.get_connector_data(self.get_connector_url('query'),
                                                               {'query': json.dumps(query_active_languages)})
                        if options_data and options_data['data']:
                            option_value = php_unserialize(options_data['data'][0]['option_value'])
                            if option_value and 'default_language' in option_value:
                                self._notice['src']['language_default'] = option_value['default_language']
                                active_langs = option_value['active_languages'].values()
                            else:
                                self._notice['src']['support']['wpml'] = False
                    if 'woocommerce-brand/main.php' in active_plugin_v_data or "wc-brand/woocommerce-brand.php" in active_plugin_v_data or 'woocommerce-brands/woocommerce-brands.php' in active_plugin_v_data or 'perfect-woocommerce-brands/perfect-woocommerce-brands.php' in active_plugin_v_data:
                        self._notice['src']['support']['manufacturers'] = True
                    if "wordpress-seo/wp-seo.php" in active_plugin_v_data:
                        self._notice['src']['support']['yoast_seo'] = True
                    if "woo-product-bundle-premium/index.php" in active_plugin_v_data or 'woo-product-bundle/index.php' in active_plugin_v_data:
                        self._notice['src']['support']['product_bundle'] = True
                    if "woocommerce-points-and-rewards/woocommerce-points-and-rewards.php" in active_plugin_v_data:
                        self._notice['src']['support']['customer_point_rewards'] = True
                    if "themedelights-addons/themedelights-addons.php" in active_plugin_v_data or "woocommerce-product-addons/woocommerce-product-addons.php" in active_plugin_v_data:
                        self._notice['src']['support']['addons'] = True
                    if active_plugin_v_data and ((
                                                         "woocommerce-sequential-order-numbers/woocommerce-sequential-order-numbers.php" in active_plugin_v_data)
                                                 or (
                                                         "custom-order-numbers-for-woocommerce/custom-order-numbers-for-woocommerce.php" in active_plugin_v_data)
                                                 or (
                                                         "sequential-order-numbers-for-woocommerce/sequential-order-numbers.php" in active_plugin_v_data)
                                                 or (
                                                         "woocommerce-sequential-order-numbers-pro/woocommerce-sequential-order-numbers-pro.php" in active_plugin_v_data)
                                                 or (
                                                         "woocommerce-sequential-order-numbers-pro/woocommerce-sequential-order-numbers.php" in active_plugin_v_data)):
                        self._notice['src']['support']['plugin_pre_ord'] = True
                    if active_plugin_v_data and 'woocommerce-order-status-manager/woocommerce-order-status-manager.php' in active_plugin_v_data:
                        self._notice['src']['support']['plugin_order_status'] = True
                    if active_plugin_v_data and 'woocommerce-status-actions/woocommerce-status-actions.php' in active_plugin_v_data:
                        self._notice['src']['support']['custom_order_status'] = True

        queries_config = {
            'orders_status': {
                'type': 'select',
                'query': "SELECT DISTINCT(`post_status`) FROM `_DBPRF_posts` WHERE `post_type` = 'shop_order'",
            },
            'permalink_structure': {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_options` WHERE option_name = 'woocommerce_permalinks' OR option_name = 'category_base'",
            }
        }
        if self._notice['src']['support']['wpml']:
            queries_config['wpml'] = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_icl_languages` WHERE code IN " + self.list_to_in_condition(active_langs)
            }
            queries_config['default_lang'] = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_options` o LEFT JOIN _DBPRF_icl_languages il ON o.option_value = il.default_locale WHERE o.`option_name` = 'WPLANG'"
            }
        if self._notice['src']['support']['plugin_order_status']:
            queries_config['orders_status'][
                'query'] = "SELECT * FROM `_DBPRF_posts` WHERE `post_type` = 'wc_order_status'"

        if self._notice['src']['support']['custom_order_status']:
            queries_config['orders_status'][
                'query'] = "SELECT * FROM `_DBPRF_posts` WHERE `post_type` = 'wc_custom_statuses' AND `post_status` = 'publish'"

        config = self.get_connector_data(url_query, {
            'serialize': True,
            'query': json.dumps(queries_config)
        })
        language_data = dict()
        order_status_data = dict()
        product_base = 'product'
        product_category_base = 'product-category'
        category_base = ''
        if config and config['result'] == 'success':
            if config['data']['orders_status']:
                for order_status_row in config['data']['orders_status']:
                    # order_status_id = 'wc-' + order_status_row['name'].lower()
                    # order_status_data[order_status_id] = order_status_row['name']
                    if self._notice['src']['support']['custom_order_status']:
                        order_status_id = 'wc-' + to_str(order_status_row['post_name'])
                        order_status_data[order_status_id] = order_status_row['post_title']
                    elif self._notice['src']['support']['plugin_order_status']:
                        order_status_id = order_status_row['post_name']
                        order_status_data[order_status_id] = order_status_row['post_title']
                    else:
                        order_status_id = order_status_row['post_status']
                        order_status_data[order_status_id] = self.get_order_status_label(
                            order_status_row['post_status'])
            else:
                order_status_data = {
                    'wc-pending': 'Pending payment',
                    'wc-processing': 'Processing',
                    'wc-on-hold': 'On hold',
                    'wc-completed': 'Completed',
                    'wc-cancelled': 'Cancelled',
                    'wc-refunded': 'Refunded',
                    'wc-failed': 'Failed'
                }
            if self._notice['src']['support']['wpml']:
                if not self._notice['src']['language_default'] and 'default_lang' in config['data'] and config['data'][
                    'default_lang']:
                    for lang_default_row in config['data']['default_lang']:
                        if lang_default_row['code']:
                            self._notice['src']['language_default'] = lang_default_row['code']

                if 'wpml' in config['data']:
                    if config['data']['wpml']:
                        for lang_row in config['data']['wpml']:
                            lang_id = lang_row["code"]
                            language_data[lang_id] = lang_row['english_name']
                    else:
                        lang_id = 'en'
                        language_data[lang_id] = "Default language"
            else:
                lang_id = 1
                language_data[lang_id] = "Default language"
            if config['data']['permalink_structure']:
                product_base_data = get_row_from_list_by_field(config['data']['permalink_structure'], 'option_name',
                                                               'woocommerce_permalinks')
                category_base_data = get_row_from_list_by_field(config['data']['permalink_structure'], 'option_name',
                                                                'category_base')
                if product_base_data:
                    option_value_data = php_unserialize(product_base_data['option_value'])
                    if option_value_data:
                        product_base = get_value_by_key_in_dict(option_value_data, 'product_base', 'product')
                        product_category_base = get_value_by_key_in_dict(option_value_data, 'category_base',
                                                                         'product-category')
                if category_base_data:
                    category_base = category_base_data['option_value']
        self._notice['src']['config']['category_base'] = product_category_base
        self._notice['src']['config']['product_category_base'] = product_category_base
        self._notice['src']['config']['product_base'] = product_base
        self._notice['src']['support']['language_map'] = True
        self._notice['src']['languages'] = language_data
        self._notice['src']['order_status'] = order_status_data
        self._notice['src']['support']['order_status_map'] = True
        self._notice['src']['support']['country_map'] = False
        self._notice['src']['support']['add_new'] = True
        self._notice['src']['support']['site_map'] = False
        self._notice['src']['support']['customer_group_map'] = False
        self._notice['src']['support']['languages_select'] = True
        self._notice['src']['support']['order_state_map'] = True

        self._notice['src']['support']['seo'] = True
        if self.is_woo2woo():
            self._notice['src']['support']['cus_pass'] = False
        else:
            self._notice['src']['support']['cus_pass'] = True
        self._notice['src']['support']['coupons'] = True
        self._notice['src']['support']['pages'] = True
        self._notice['src']['support']['seo_301'] = True
        self._notice['src']['config']['seo_module'] = self.get_list_seo()
        return response_success()

    def display_config_target(self):
        url_query = self.get_connector_url('query')
        self._notice['target']['language_default'] = 1
        self._notice['target']['category_root'] = 1
        storage_cat_data = dict()
        storage_cat_data[self._notice['target']['language_default']] = 0
        self._notice['target']['store_category'] = storage_cat_data
        self._notice['target']['support']['site_map'] = False
        self._notice['target']['support']['category_map'] = False
        self._notice['target']['support']['attribute_map'] = False
        self._notice['target']['support']['wpml'] = False
        self._notice['target']['support']['wpml_currency'] = False
        self._notice['target']['support']['product_bundle'] = False
        self._notice['target']['support']['yoast_seo'] = False
        self._notice['target']['support']['addons'] = False
        self._notice['target']['support']['customer_point_rewards'] = False
        self._notice['target']['support']['polylang'] = False
        self._notice['target']['support']['polylang_product'] = False
        self._notice['target']['support']['polylang_category'] = False
        self._notice['target']['support']['plugin_woo_admin'] = False
        self._notice['target']['support']['custom_order_status'] = False
        self._notice['target']['currency_map'] = dict()
        query_active_plugins = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_options WHERE option_name = 'active_plugins'"
        }
        active_plugins = self.get_connector_data(self.get_connector_url('query'),
                                                 {'query': json.dumps(query_active_plugins)})
        active_langs = list()
        if active_plugins and active_plugins['data']:
            active_plugin = active_plugins['data'][0]
            active_plugin_v = active_plugin['option_value']
            if active_plugin_v:
                active_plugin_v_data = php_unserialize(active_plugin_v)
                if active_plugin_v_data and isinstance(active_plugin_v_data, dict):
                    active_plugin_v_data = list(active_plugin_v_data.values())
                if active_plugin_v_data and "woocommerce-multilingual/wpml-woocommerce.php" in active_plugin_v_data:
                    self._notice['target']['support']['wpml'] = True
                    query_active_languages = {
                        'type': 'select',
                        'query': "SELECT * FROM `_DBPRF_options` WHERE `option_name` = 'icl_sitepress_settings'"
                    }
                    options_data = self.get_connector_data(self.get_connector_url('query'),
                                                           {'query': json.dumps(query_active_languages)})
                    if options_data and options_data['data']:
                        option_value = php_unserialize(options_data['data'][0]['option_value'])
                        if option_value and 'default_language' in option_value:
                            self._notice['target']['language_default'] = option_value['default_language']
                            active_langs = option_value['active_languages'].values()

                    query_active_currency = {
                        'type': 'select',
                        'query': "SELECT * FROM `_DBPRF_options` WHERE `option_name` = '_wcml_settings'"
                    }
                    options_currency_data = self.get_connector_data(self.get_connector_url('query'),
                                                                    {'query': json.dumps(query_active_currency)})
                    if options_currency_data and options_currency_data['data']:
                        currency_value = php_unserialize(options_currency_data['data'][0]['option_value'])
                        if currency_value and 'enable_multi_currency' in currency_value and to_int(
                                currency_value['enable_multi_currency']) >= 2:
                            self._notice['target']['support']['wpml_currency'] = True
                            if 'default_currencies' in currency_value and currency_value['default_currencies']:
                                self._notice['target']['currency_map'] = currency_value['default_currencies']
                        else:
                            self._notice['target']['support']['wpml_currency'] = False
                woo_brands = [
                    {'name': 'woocommerce-brand/main.php'},
                    {'name': 'wc-brand/woocommerce-brand.php'},
                    {'name': 'martfury-addons/martfury-addons.php', 'taxonomy': 'product_brand'},
                    {'name': 'woocommerce-brands/woocommerce-brands.php', 'taxonomy': 'product_brand'},
                    {'name': 'brands-for-woocommerce/woocommerce-brand.php', 'taxonomy': 'berocket_brand'},
                    {'name': 'perfect-woocommerce-brands/main.php', 'taxonomy': 'pwb-brand'},
                    {'name': 'perfect-woocommerce-brands/perfect-woocommerce-brands.php', 'taxonomy': 'pwb-brand'},
                ]
                self._notice['target']['config']['brand_taxonomy'] = 'product_brand'
                for brand in woo_brands:
                    if brand['name'] in active_plugin_v_data:
                        self._notice['target']['support']['plugin_manufacturers'] = True
                        if brand.get('taxonomy'):
                            self._notice['target']['config']['brand_taxonomy'] = brand['taxonomy']
                        break
                if active_plugin_v_data and ((
                                                     "woocommerce-sequential-order-numbers/woocommerce-sequential-order-numbers.php" in active_plugin_v_data)
                                             or (
                                                     "custom-order-numbers-for-woocommerce/custom-order-numbers-for-woocommerce.php" in active_plugin_v_data)
                                             or (
                                                     "sequential-order-numbers-for-woocommerce/sequential-order-numbers.php" in active_plugin_v_data)
                                             or (
                                                     "woocommerce-sequential-order-numbers-pro/woocommerce-sequential-order-numbers-pro.php" in active_plugin_v_data)):
                    self._notice['target']['support']['plugin_pre_ord'] = True
                if active_plugin_v_data and "wordpress-seo/wp-seo.php" in active_plugin_v_data:
                    self._notice['target']['support']['yoast_seo'] = True
                if "themedelights-addons/themedelights-addons.php" in active_plugin_v_data or "woocommerce-product-addons/woocommerce-product-addons.php" in active_plugin_v_data:
                    self._notice['target']['support']['addons'] = True
                if "leurlrewrite/leurlrewrite.php" in active_plugin_v_data:
                    self._notice['target']['support']['plugin_seo'] = True
                    self._notice['target']['support']['plugin_seo_301'] = True
                if "leprespass/leprespass.php" in active_plugin_v_data:
                    self._notice['target']['support']['plugin_cus_pass'] = True
                if "woocommerce-admin/woocommerce-admin.php" in active_plugin_v_data:
                    self._notice['target']['support']['plugin_woo_admin'] = True
                if "woo-product-bundle-premium/index.php" in active_plugin_v_data or 'woo-product-bundle/index.php' in active_plugin_v_data:
                    self._notice['target']['support']['product_bundle'] = True
                if "woocommerce-points-and-rewards/woocommerce-points-and-rewards.php" in active_plugin_v_data:
                    self._notice['target']['support']['customer_point_rewards'] = True
                if 'polylang/polylang.php' in active_plugin_v_data:
                    self._notice['target']['support']['polylang'] = True
                if 'woocommerce-status-actions/woocommerce-status-actions.php' in active_plugin_v_data:
                    self._notice['target']['support']['custom_order_status'] = True

        queries_config = {
            'orders_status': {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_term_taxonomy` AS term_taxonomy LEFT JOIN _DBPRF_terms AS terms ON term_taxonomy.term_id = terms.term_id WHERE term_taxonomy.taxonomy = 'shop_order_status'",
            },
        }
        if self._notice['target']['support']['wpml']:
            queries_config['wpml'] = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_icl_languages` WHERE code IN " + self.list_to_in_condition(active_langs)
            }
            queries_config['default_lang'] = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_options` o LEFT JOIN _DBPRF_icl_languages il ON o.option_value = il.default_locale WHERE o.`option_name` = 'WPLANG' and o.`option_value` != '' "
            }
        if self._notice['target']['support']['polylang']:
            queries_config['polylang'] = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_terms` as t LEFT JOIN `_DBPRF_term_taxonomy` as tx ON t.term_id = tx.term_id WHERE tx.taxonomy = 'language'"
            }
            queries_config['polylang_categories'] = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_terms` as t LEFT JOIN `_DBPRF_term_taxonomy` as tx ON t.term_id = tx.term_id WHERE tx.taxonomy = 'term_language'"
            }
        if self._notice['target']['support']['custom_order_status']:
            queries_config['custom_order_status'] = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_posts` WHERE `post_type` = 'wc_custom_statuses' AND `post_status` = 'publish'"
            }

        config = self.select_multiple_data_connector(queries_config)
        if 'polylang' in config['data'] and not config['data']['polylang']:
            self._notice['target']['support']['polylang'] = False
        language_data = dict()
        order_status_data = dict()
        polylang_products = dict()
        polylang_categories = dict()
        if config and config['result'] == 'success':
            if self._notice['target']['support']['custom_order_status'] and config['data'][
                'custom_order_status'] and to_len(config['data']['custom_order_status']) > 0:
                for order_status_row in config['data']['custom_order_status']:
                    order_status_id = 'wc-' + to_str(order_status_row['post_name'])
                    order_status_data[order_status_id] = order_status_row['post_title']
            elif config['data']['orders_status'] and to_len(config['data']['orders_status']) > 0:
                for order_status_row in config['data']['orders_status']:
                    order_status_id = 'wc-' + to_str(order_status_row['name']).lower()
                    order_status_data[order_status_id] = order_status_row['name']
            else:
                order_status_data = {
                    'wc-pending': 'Pending payment',
                    'wc-processing': 'Processing',
                    'wc-on-hold': 'On hold',
                    'wc-completed': 'Completed',
                    'wc-cancelled': 'Cancelled',
                    'wc-refunded': 'Refunded',
                    'wc-failed': 'Failed'
                }
            if self._notice['target']['support']['wpml']:
                if not self._notice['target']['language_default'] and 'default_lang' in config['data'] and \
                        config['data']['default_lang']:
                    for lang_default_row in config['data']['default_lang']:
                        if lang_default_row['code']:
                            self._notice['target']['language_default'] = lang_default_row['code']

                if 'wpml' in config['data']:
                    if config['data']['wpml']:
                        for lang_row in config['data']['wpml']:
                            lang_id = lang_row["code"]
                            language_data[lang_id] = lang_row['english_name']
                    else:
                        lang_id = 'en'
                        language_data[lang_id] = "Default language"
            elif self._notice['target']['support']['polylang']:
                if not self._notice['target']['language_default'] and 'default_lang' in config['data'] and \
                        config['data']['default_lang']:
                    for lang_default_row in config['data']['default_lang']:
                        if lang_default_row['code']:
                            self._notice['target']['language_default'] = lang_default_row['code']
                if 'polylang' in config['data']:
                    if config['data']['polylang']:
                        self._notice['target']['language_default'] = 'en'
                        for lang_row in config['data']['polylang']:
                            lang_id = lang_row['slug']
                            language_data[lang_id] = lang_row['name']
                            lang_product = lang_row['slug']
                            polylang_products[lang_product] = lang_row['term_taxonomy_id']
                    if config['data']['polylang_categories']:
                        for lang_row in config['data']['polylang_categories']:
                            lang_category = lang_row['slug'].replace('pll_', '')
                            polylang_categories[lang_category] = lang_row['term_taxonomy_id']
                else:
                    lang_id = 'en'
                    language_data[lang_id] = "Default language"
            else:
                lang_id = 1
                language_data[lang_id] = "Default language"
        else:
            order_status_data = {
                'wc-pending': 'Pending payment',
                'wc-processing': 'Processing',
                'wc-on-hold': 'On hold',
                'wc-completed': 'Completed',
                'wc-cancelled': 'Cancelled',
                'wc-refunded': 'Refunded',
                'wc-failed': 'Failed'
            }
            lang_id = 1
            language_data[lang_id] = "Default language"
        self._notice['target']['support']['manufacturers'] = True
        self._notice['target']['support']['check_manufacturers'] = True
        # self._notice['target']['support']['yoast_seo'] = False
        self._notice['target']['support']['pre_ord'] = True
        self._notice['target']['support']['check_pre_ord'] = True
        self._notice['target']['support']['seo'] = True
        self._notice['target']['support']['check_seo'] = True
        self._notice['target']['support']['seo_301'] = True
        self._notice['target']['support']['check_seo_301'] = True
        self._notice['target']['support']['cus_pass'] = True
        self._notice['target']['support']['check_cus_pass'] = True
        self._notice['target']['support']['language_map'] = True
        self._notice['target']['languages'] = language_data
        self._notice['target']['order_status'] = order_status_data
        self._notice['target']['support']['order_status_map'] = True
        self._notice['target']['support']['country_map'] = False
        self._notice['target']['support']['add_new'] = True
        self._notice['target']['support']['coupons'] = True
        self._notice['target']['support']['blogs'] = True
        self._notice['target']['support']['pages'] = True
        self._notice['target']['support']['site_map'] = False
        self._notice['target']['support']['pre_prd'] = False
        self._notice['target']['support']['pre_cus'] = False
        self._notice['target']['support']['img_des'] = True
        self._notice['target']['support']['customer_group_map'] = False
        self._notice['target']['support']['languages_select'] = True
        self._notice['target']['support']['update_latest_data'] = True
        self._notice['target']['config']['entity_update']['products'] = True
        self._notice['target']['support']['polylang_product'] = polylang_products
        self._notice['target']['support']['polylang_category'] = polylang_categories
        return response_success()

    def get_query_display_import_source(self, update=False):
        compare_condition = ' > '
        if update:
            compare_condition = ' <= '
        prefix = self._notice['src']['config']['table_prefix']
        if self._notice['src']['config'].get('site_id'):
            prefix = to_str(prefix).replace(to_str(self._notice['src']['config'].get('site_id')) + '_', '')

        queries = {
            'taxes': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_options WHERE option_name = 'woocommerce_tax_classes'",
            },
            'manufacturers': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_term_taxonomy WHERE (taxonomy = 'product_brand' OR taxonomy = 'brand' OR taxonomy = 'pwb-brand') AND term_id " + compare_condition + to_str(
                    self._notice['process']['manufacturers']['id_src']),
            },
            'categories': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_term_taxonomy WHERE taxonomy = 'product_cat' AND term_id " + compare_condition + to_str(
                    self._notice['process']['categories']['id_src']),
            },
            'products': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_posts WHERE post_type = 'product' AND post_status NOT IN ('inherit','auto-draft') AND ID " + compare_condition + to_str(
                    self._notice['process']['products']['id_src']),
            },
            'customers': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM " + prefix + "users u LEFT JOIN " + prefix + "usermeta um ON u.ID = um.user_id WHERE (um.meta_key = '_DBPRF_capabilities' AND um.meta_value LIKE '%customer%' OR um.meta_value LIKE '%subscriber%') AND u.ID " + compare_condition + to_str(
                    self._notice['process']['customers']['id_src']),
            },
            'orders': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_posts WHERE post_type = 'shop_order' AND post_status NOT IN ('inherit','auto-draft') AND ID " + compare_condition + to_str(
                    self._notice['process']['orders']['id_src']),
            },
            'reviews': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_comments AS cm,_DBPRF_posts AS p WHERE cm.comment_post_ID = p.ID AND p.post_type = 'product' AND cm.comment_ID " + compare_condition + to_str(
                    self._notice['process']['reviews']['id_src']),
            },
            'pages': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_posts WHERE post_type = 'page' AND ID " + compare_condition + to_str(
                    self._notice['process']['pages']['id_src']),
            },
            'coupons': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_posts WHERE post_type = 'shop_coupon' AND ID " + compare_condition + to_str(
                    self._notice['process']['coupons']['id_src']),

            },
            'blogs': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_posts WHERE post_type = 'post' AND ID " + compare_condition + to_str(
                    self._notice['process']['blogs']['id_src']),

            },
        }
        if self._notice['src']['support']['wpml']:
            queries['categories'] = {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_term_taxonomy tt LEFT JOIN _DBPRF_icl_translations il ON tt.term_taxonomy_id = il.element_id "
                         "WHERE il.`element_type` = 'tax_product_cat' and il.`source_language_code` IS NULL and tt.taxonomy = 'product_cat' and tt.term_taxonomy_id " + compare_condition + to_str(
                    self._notice['process']['categories']['id_src']),
            }
            queries['products'] = {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_posts p LEFT JOIN _DBPRF_icl_translations il ON p.ID = il.element_id "
                         "WHERE il.`source_language_code` is NULL and il.`element_type` = 'post_product' and p.post_type = 'product' AND p.post_status NOT IN ('inherit','auto-draft') AND p.ID " + compare_condition + to_str(
                    self._notice['process']['products']['id_src']),
            }
        return queries

    def display_import_source(self):
        if self._notice['config']['add_new']:
            self.display_recent_data()

        queries = self.get_query_display_import_source()
        count = self.get_connector_data(self.get_connector_url('query'), {
            'serialize': True,
            'query': json.dumps(queries)
        })
        if (not count) or (count['result'] != 'success'):
            return response_error()
        real_totals = dict()
        for key, row in count['data'].items():
            total = 0
            if key == 'taxes':
                if row and to_len(row) > 0:
                    taxes = row[0]['option_value'].splitlines()
                    total = (to_len(taxes) + 1) if taxes else 1
            else:
                total = self.list_to_count_import(row, 'count')
            real_totals[key] = total
        for key, total in real_totals.items():
            self._notice['process'][key]['total'] = total
        return response_success()

    def display_update_source(self):
        queries = self.get_query_display_import_source(True)
        count = self.select_multiple_data_connector(queries, 'count')

        if (not count) or (count['result'] != 'success'):
            return response_error()
        real_totals = dict()
        for key, row in count['data'].items():
            total = 0
            if key == 'taxes':
                if row and to_len(row) > 0:
                    taxes = row[0]['option_value'].splitlines()
                    total = (to_len(taxes) + 1) if taxes else 1
            else:
                total = self.list_to_count_import(row, 'count')
            real_totals[key] = total
        for key, total in real_totals.items():
            self._notice['process'][key]['total_update'] = total
        return response_success()

    def display_import_target(self):
        return response_success()

    def prepare_import_target(self):
        parent = super().prepare_import_target()
        if parent['result'] != 'success':
            return parent
        query_active_plugins = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_options WHERE option_name = 'active_plugins'"
        }
        active_plugins = self.get_connector_data(self.get_connector_url('query'),
                                                 {'query': json.dumps(query_active_plugins)})
        if active_plugins and active_plugins['data']:
            active_plugin = active_plugins['data'][0]
            active_plugin_v = active_plugin['option_value']
            if active_plugin_v:
                active_plugin_v_data = php_unserialize(active_plugin_v)
                if active_plugin_v_data and isinstance(active_plugin_v_data, dict):
                    active_plugin_v_data = list(active_plugin_v_data.values())
                if active_plugin_v_data and "woocommerce-multilingual/wpml-woocommerce.php" in active_plugin_v_data:
                    self._notice['target']['support']['wpml'] = True
                    query_active_languages = {
                        'type': 'select',
                        'query': "SELECT * FROM `_DBPRF_options` WHERE `option_name` = 'icl_sitepress_settings'"
                    }
                    options_data = self.get_connector_data(self.get_connector_url('query'),
                                                           {'query': json.dumps(query_active_languages)})
                    if options_data and options_data['data']:
                        option_value = php_unserialize(options_data['data'][0]['option_value'])
                        if option_value and 'default_language' in option_value:
                            self._notice['target']['language_default'] = option_value['default_language']
                woo_brands = [
                    {'name': 'woocommerce-brand/main.php'},
                    {'name': 'wc-brand/woocommerce-brand.php'},
                    {'name': 'woocommerce-brands/woocommerce-brands.php'},
                    {'name': 'brands-for-woocommerce/woocommerce-brand.php', 'taxonomy': 'berocket_brand'},
                    {'name': 'perfect-woocommerce-brands/main.php', 'taxonomy': 'pwb-brand'},
                ]
                for brand in woo_brands:
                    if brand['name'] in active_plugin_v_data:
                        self._notice['target']['support']['plugin_manufacturers'] = False
                        if brand.get('taxonomy'):
                            self._notice['target']['config']['brand_taxonomy'] = brand['taxonomy']
                        break
                if active_plugin_v_data and ((
                                                     "woocommerce-sequential-order-numbers/woocommerce-sequential-order-numbers.php" in active_plugin_v_data) or (
                                                     "custom-order-numbers-for-woocommerce/custom-order-numbers-for-woocommerce.php" in active_plugin_v_data) or (
                                                     "sequential-order-numbers-for-woocommerce/sequential-order-numbers.php" in active_plugin_v_data)):
                    self._notice['target']['support']['plugin_pre_ord'] = True
                if active_plugin_v_data and "wordpress-seo/wp-seo.php" in active_plugin_v_data:
                    self._notice['target']['support']['yoast_seo'] = True
                if "themedelights-addons/themedelights-addons.php" in active_plugin_v_data or "woocommerce-product-addons/woocommerce-product-addons.php" in active_plugin_v_data:
                    self._notice['target']['support']['addons'] = True
                if "leurlrewrite/leurlrewrite.php" in active_plugin_v_data:
                    self._notice['target']['support']['plugin_seo'] = True
                    self._notice['target']['support']['plugin_seo_301'] = True
                if "leprespass/leprespass.php" in active_plugin_v_data:
                    self._notice['target']['support']['plugin_cus_pass'] = True
                if "woo-product-bundle-premium/index.php" in active_plugin_v_data:
                    self._notice['target']['support']['product_bundle'] = True
                if "woocommerce-admin/woocommerce-admin.php" in active_plugin_v_data:
                    self._notice['target']['support']['plugin_woo_admin'] = True
        if self._notice['config']['seo'] or self._notice['config']['seo_301']:
            query = self.dict_to_create_table_sql(self.lecm_rewrite_table_construct())
            self.query_data_connector({'type': 'query', 'query': query['query']})
        if self._notice['target']['support']['wpml'] or self._notice['target']['support'].get('polylang'):
            add_column = "ALTER TABLE " + self.get_table_name(TABLE_MAP) + " ADD `lang` VARCHAR(255)"
            self.query_raw(add_column)
            add_column = "ALTER TABLE _DBPRF_lecm_rewrite ADD `lang` VARCHAR(255)"
            self.query_data_connector({'type': 'query', 'query': add_column})
        return response_success()

    def display_confirm_target(self):
        self._notice['target']['clear']['function'] = 'clear_target_taxes'
        self._notice['target']['clear_demo']['function'] = 'clear_target_products_demo'
        return response_success()

    # TODO clear demo
    def clear_target_manufacturers_demo(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_categories_demo',
        }
        self._notice['target']['clear_demo'] = next_clear
        if not self._notice['config']['manufacturers']:
            return next_clear
        where = {
            'migration_id': self._migration_id,
            'type': self.TYPE_MANUFACTURER
        }
        manufacturers = self.select_obj(TABLE_MAP, where)
        manufacturer_ids = list()
        if manufacturers['result'] == 'success':
            manufacturer_ids = duplicate_field_value_from_list(manufacturers['data'], 'id_desc')

        if not manufacturer_ids:
            return next_clear
        manufacturer_id_con = self.list_to_in_condition(manufacturer_ids)

        taxonomy_meta_table = 'termmeta'
        collections_query = {
            'type': 'select',
            'query': "SELECT * FROM `_DBPRF_term_taxonomy` WHERE taxonomy = 'product_brand' OR taxonomy = 'brand' OR taxonomy = 'pwb-brand' AND term_id IN " + manufacturer_id_con
        }
        manufacturers = self.get_connector_data(self.get_connector_url('query'),
                                                {'query': json.dumps(collections_query)})
        if manufacturers['data']:
            all_queries = list()
            taxonomy_ids = duplicate_field_value_from_list(manufacturers['data'], 'term_taxonomy_id')
            all_queries.append({
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_" + taxonomy_meta_table + "` WHERE term_id IN " + manufacturer_id_con
            })
            all_queries.append({
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_terms` WHERE term_id IN " + manufacturer_id_con
            })
            all_queries.append({
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_term_taxonomy` WHERE term_taxonomy_id IN " + self.list_to_in_condition(
                    taxonomy_ids)
            })
            if all_queries:
                self.import_multiple_data_connector(all_queries, 'cleardemo')
        return self._notice['target']['clear_demo']

    def clear_target_categories_demo(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_products_demo',
        }
        self._notice['target']['clear_demo'] = next_clear
        if not self._notice['config']['categories']:
            return next_clear
        where = {
            'migration_id': self._migration_id,
            'type': self.TYPE_CATEGORY
        }
        categories = self.select_obj(TABLE_MAP, where)
        category_ids = list()
        if categories['result'] == 'success':
            category_ids = duplicate_field_value_from_list(categories['data'], 'id_desc')

        if not category_ids:
            return next_clear
        category_id_con = self.list_to_in_condition(category_ids)
        taxonomy_meta_table = 'termmeta'

        collections_query = {
            'type': 'select',
            'query': "SELECT * FROM `_DBPRF_term_taxonomy` WHERE taxonomy = 'product_cat' OR taxonomy = 'post_cat' AND term_id IN " + category_id_con
        }
        categories = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(collections_query)})
        if categories['data']:
            all_queries = list()
            taxonomy_ids = duplicate_field_value_from_list(categories['data'], 'term_taxonomy_id')
            all_queries.append({
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_" + taxonomy_meta_table + "` WHERE term_id IN " + category_id_con
            })
            all_queries.append({
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_terms` WHERE term_id IN " + category_id_con
            })
            all_queries.append({
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_term_taxonomy` WHERE term_taxonomy_id IN " + self.list_to_in_condition(
                    taxonomy_ids)
            })
            if self._notice['target']['support']['wpml']:
                clear_table = self.get_connector_data(self.get_connector_url('query'), {
                    'query': json.dumps({
                        'type': 'query',
                        'query': "DELETE FROM `_DBPRF_icl_translations` "
                                 + "WHERE element_type = 'tax_product_cat' AND element_id IN "
                                 + category_id_con
                    })
                })
            if self._notice['config']['seo'] or self._notice['config']['seo_301']:
                clear_table = self.get_connector_data(self.get_connector_url('query'), {
                    'query': json.dumps({
                        'type': 'query',
                        'query': "DELETE FROM `_DBPRF_lecm_rewrite` where type = 'category' and type_id IN " + category_id_con
                    })
                })
            if all_queries:
                self.import_multiple_data_connector(all_queries, 'cleardemo')
        return next_clear

    def clear_target_products_demo(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_orders_demo',
        }
        if not self._notice['config']['products']:
            self._notice['target']['clear_demo'] = next_clear
            return next_clear
        where = {
            'migration_id': self._migration_id,
            'type': self.TYPE_PRODUCT
        }
        products = self.select_page(TABLE_MAP, where, self.LIMIT_CLEAR_DEMO)
        product_ids = list()
        if products['result'] == 'success':
            product_ids = duplicate_field_value_from_list(products['data'], 'id_desc')
        if not product_ids:
            self._notice['target']['clear_demo'] = next_clear
            return next_clear
        product_id_con = self.list_to_in_condition(product_ids)
        collections_query = {
            'type': 'select',
            'query': "SELECT * FROM `_DBPRF_posts` "
                     + " WHERE ID IN " + product_id_con + " OR post_parent IN " + product_id_con
        }
        products = self.get_connector_data(self.get_connector_url('query'),
                                           {'query': json.dumps(collections_query)})
        all_post_id = list()
        if products['data']:
            all_post_id = duplicate_field_value_from_list(products['data'], 'ID')
        all_collections_query = {
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_posts` "
                     + " WHERE ID IN " + self.list_to_in_condition(all_post_id)
        }
        clear_table = self.get_connector_data(self.get_connector_url('query'),
                                              {'query': json.dumps(all_collections_query)})
        all_meta_query = {
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_post_meta`"
                     + " WHERE post_id IN " + self.list_to_in_condition(all_post_id)
        }
        clear_table = self.get_connector_data(self.get_connector_url('query'),
                                              {'query': json.dumps(all_meta_query)})
        where = {
            'migration_id': self._migration_id,
            'type': self.TYPE_OPTION
        }
        attibutes = self.select_obj(TABLE_MAP, where)
        attibutes_ids = list()
        attibutes_codes = list()
        if attibutes['result'] == 'success':
            attibutes_ids = duplicate_field_value_from_list(attibutes['data'], 'id_desc')
            attibutes_codes = duplicate_field_value_from_list(attibutes['data'], 'value')
        if attibutes_ids:
            del_transient_attr_query = {
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_woocommerce_attribute_taxonomies` WHERE attribute_id IN "
                         + self.list_to_in_condition(attibutes_ids)
            }
            self.get_connector_data(self.get_connector_url('query'),
                                    {'query': json.dumps(del_transient_attr_query)})
            term_query = {
                "type": "select",
                "query": "SELECT * FROM `_DBPRF_term_taxonomy` tt LEFT JOIN `_DBPRF_terms` t ON tt.term_id = t.term_id "
                         + " WHERE tt.taxonomy IN "
                         + self.list_to_in_condition(attibutes_codes)
            }
            terms = self.get_connector_data(self.get_connector_url('query'),
                                            {'query': json.dumps(term_query)})
            if (terms['data']):
                term_ids = duplicate_field_value_from_list(terms['data'], 'term_id')
                taxonomy_ids = duplicate_field_value_from_list(terms['data'], 'term_taxonomy_id')
                del_transient_attr_query = {
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_term_taxonomy` WHERE term_taxonomy_id IN "
                             + self.list_to_in_condition(taxonomy_ids)
                }
                self.get_connector_data(self.get_connector_url('query'),
                                        {'query': json.dumps(del_transient_attr_query)})
                del_transient_attr_query = {
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_terms` WHERE term_id IN "
                             + self.list_to_in_condition(term_ids)
                }
                self.get_connector_data(self.get_connector_url('query'),
                                        {'query': json.dumps(del_transient_attr_query)})

        if self._notice['target']['support']['wpml']:
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_icl_translations` "
                             + " WHERE element_type = 'post_product' AND element_id IN " + product_id_con
                })
            })
        if self._notice['config']['seo'] or self._notice['config']['seo_301']:
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_lecm_rewrite` where type = 'product' and type_id IN " + product_id_con
                })
            })
        self.delete_map_demo(self.TYPE_PRODUCT, product_ids)
        if product_ids and to_len(product_ids) < self.LIMIT_CLEAR_DEMO:
            self._notice['target']['clear_demo'] = next_clear
            return next_clear
        return self._notice['target']['clear_demo']

    def clear_target_customers_demo(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_orders_demo',
        }
        self._notice['target']['clear_demo'] = next_clear
        if not self._notice['config']['customers']:
            return next_clear
        where = {
            'migration_id': self._migration_id,
            'type': self.TYPE_CUSTOMER
        }
        customers = self.select_obj(TABLE_MAP, where)
        customer_ids = list()
        if customers['result'] == 'success':
            customer_ids = duplicate_field_value_from_list(customers['data'], 'id_desc')
        if not customer_ids:
            return next_clear
        customer_id_con = self.list_to_in_condition(customer_ids)
        del_user_query = "DELETE FROM _DBPRF_users WHERE ID IN " + customer_id_con
        clear_table = self.get_connector_data(self.get_connector_url('query'), {
            'query': json.dumps({
                'type': 'query', 'query': del_user_query
            })
        })
        if (not clear_table) or (clear_table['result'] != 'success') or (not clear_table['data']):
            self.log("Clear data failed. Error: Could not empty customers ", 'clear')
        del_user_meta_query = "DELETE FROM _DBPRF_usermeta WHERE user_id IN " + customer_id_con
        clear_table = self.get_connector_data(self.get_connector_url('query'), {
            'query': json.dumps({
                'type': 'query', 'query': del_user_meta_query
            })
        })

        if self._notice['target']['support'].get('plugin_woo_admin') or self.convert_version(
                self._notice['target']['config']['version'], 2) > 399:
            del_customer_lookup_query = "DELETE FROM _DBPRF_wc_customer_lookup WHERE user_id IN " + customer_id_con
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query', 'query': del_customer_lookup_query
                })
            })

        return next_clear

    def clear_target_orders_demo(self):
        next_clear = {
            'result': 'success',
            'function': 'clear_target_reviews_demo',
        }
        if not self._notice['config']['orders']:
            self._notice['target']['clear_demo'] = next_clear

            return next_clear
        where = {
            'migration_id': self._migration_id,
            'type': self.TYPE_ORDER
        }
        orders = self.select_page(TABLE_MAP, where, self.LIMIT_CLEAR_DEMO)

        order_ids = list()
        if orders['result'] == 'success':
            order_ids = duplicate_field_value_from_list(orders['data'], 'id_desc')
        if not order_ids:
            self._notice['target']['clear_demo'] = next_clear
            return next_clear
        all_collections_query = {
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_posts` WHERE post_type IN ('shop_order', 'shop_order_refund') AND ID IN " + self.list_to_in_condition(
                order_ids)
        }
        clear_table = self.get_connector_data(self.get_connector_url('query'),
                                              {'query': json.dumps(all_collections_query)})

        # clear meta post(orders)
        all_meta_query = {
            'type': 'select',
            'query': "DELETE FROM `_DBPRF_post_meta` WHERE post_id IN " + self.list_to_in_condition(order_ids)
        }
        clear_table = self.get_connector_data(self.get_connector_url('query'),
                                              {'query': json.dumps(all_meta_query)})
        self.delete_map_demo(self.TYPE_ORDER, order_ids)
        if order_ids and to_len(order_ids) < self.LIMIT_CLEAR_DEMO:
            self._notice['target']['clear_demo'] = next_clear
            return next_clear
        return self._notice['target']['clear_demo']

    def clear_target_reviews_demo(self):
        next_clear = {
            'result': 'success',
            'function': 'clear_target_pages_demo',
        }
        self._notice['target']['clear_demo'] = next_clear
        if not self._notice['config']['reviews']:
            return next_clear
        where = {
            'migration_id': self._migration_id,
            'type': self.TYPE_REVIEW
        }
        reviews = self.select_obj(TABLE_MAP, where)
        review_ids = list()
        if reviews['result'] == 'success':
            review_ids = duplicate_field_value_from_list(reviews['data'], 'id_desc')
        if not review_ids:
            return next_clear
        tables = [
            'commentmeta',
            'comments'
        ]
        for table in tables:
            where = ''
            if table == 'comments':
                where = " WHERE comment_ID IN " + self.list_to_in_condition(review_ids)
            if table == 'commentmeta':
                where = " WHERE comment_id IN " + self.list_to_in_condition(review_ids)

            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`" + where
                })
            })
            if (not clear_table) or (clear_table['result'] != 'success'):
                self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
                continue

    # TODO: clear
    def clear_target_taxes(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_manufacturers',
            'msg': ''
        }
        if not self._notice['config']['taxes']:
            self._notice['target']['clear'] = next_clear
            return next_clear
        tables = [
            'options',
            'woocommerce_tax_rates',
            'woocommerce_tax_rate_locations',
            'wc_tax_rate_classes'
        ]
        for table in tables:
            if table == 'options':
                clear_table = self.get_connector_data(self.get_connector_url('query'), {
                    'query': json.dumps({
                        'type': 'query',
                        'query': "UPDATE `_DBPRF_" + table + "` SET `option_value` = '' WHERE `option_name` = 'woocommerce_tax_classes'"
                    })
                })
                continue
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_" + table + "` WHERE 1"
                })
            })
            if (not clear_table) or (clear_table['result'] != 'success'):
                self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
                continue
        self._notice['target']['clear'] = next_clear
        return next_clear

    def clear_target_manufacturers(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_categories',
            'msg': ''
        }
        if not self._notice['config']['manufacturers']:
            self._notice['target']['clear'] = next_clear
            return next_clear

        taxonomy_meta_table = 'termmeta'
        taxonomy = 'berocket_brand'
        if self._notice['target']['config'].get('brand_taxonomy'):
            taxonomy = self._notice['target']['config']['brand_taxonomy']
        tables = ['termmeta', 'terms', 'term_relationships', 'term_taxonomy']
        for table in tables:
            where = ''
            if table in ['termmeta', 'terms']:
                where = " term_id IN (SELECT term_id FROM `_DBPRF_term_taxonomy` WHERE taxonomy = " + self.escape(
                    taxonomy) + " )"
            if table in ['term_relationships']:
                where = " term_taxonomy_id IN (SELECT term_taxonomy_id FROM `_DBPRF_term_taxonomy` WHERE taxonomy = " + self.escape(
                    taxonomy) + " )"
            if table == 'term_taxonomy':
                where = " taxonomy = " + self.escape(taxonomy)
            query = "DELETE FROM `_DBPRF_" + table + "` WHERE " + where
            clear_table = self.query_data_connector({'type': 'delete', 'query': query})
            if (not clear_table) or (clear_table['result'] != 'success'):
                self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
                continue
        if self._notice['target']['support']['yoast_seo']:
            query_wpseo = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_options` WHERE `option_name` = 'wpseo_taxonomy_meta'"
            }
            options_data = self.get_connector_data(self.get_connector_url('query'),
                                                   {'query': json.dumps(query_wpseo)})
            if options_data and options_data['data']:
                option_value = php_unserialize(options_data['data'][0]['option_value'])
                if taxonomy in option_value:
                    option_value[taxonomy] = dict()
                data_set = {
                    'option_value': php_serialize(option_value)
                }
                where = {
                    'option_id': options_data['data'][0]['option_id'],
                    'option_name': 'wpseo_taxonomy_meta'
                }
                update_query = self.create_update_query_connector('options', data_set, where)
                wpseo_taxonomy_clear = self.import_data_connector(update_query, 'manufacturer')
        self._notice['target']['clear'] = next_clear
        return next_clear

    def clear_target_categories(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_products',
            'msg': ''
        }
        if not self._notice['config']['categories']:
            self._notice['target']['clear'] = next_clear
            return next_clear
        taxonomy_meta_table = 'termmeta'
        while self._check_categories_exists():
            all_collections_query = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_term_taxonomy` WHERE taxonomy = 'product_cat' OR taxonomy = 'post_cat' LIMIT 200"
            }
            categories = self.get_connector_data(self.get_connector_url('query'),
                                                 {'query': json.dumps(all_collections_query)})
            if not categories:
                return next_clear
            term_ids = duplicate_field_value_from_list(categories['data'], 'term_id')

            taxonomy_ids = duplicate_field_value_from_list(categories['data'], 'term_taxonomy_id')
            taxnomy_query = {
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_" + taxonomy_meta_table + "` WHERE term_id IN " + self.list_to_in_condition(
                    term_ids)
            }
            self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(taxnomy_query)})
            self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps({
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_terms` WHERE term_id IN " + self.list_to_in_condition(
                    term_ids)
            })})

            self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps({
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_term_taxonomy` WHERE term_taxonomy_id IN " + self.list_to_in_condition(
                    taxonomy_ids)
            })
            })
        # end for
        if self._notice['target']['support']['wpml']:
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_icl_translations` where element_type = 'tax_product_cat'"
                })
            })
        if self._notice['config']['seo'] or self._notice['config']['seo_301']:
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_lecm_rewrite` where type = 'category'"
                })
            })
        if self._notice['target']['support']['yoast_seo']:
            query_wpseo = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_options` WHERE `option_name` = 'wpseo_taxonomy_meta'"
            }
            options_data = self.get_connector_data(self.get_connector_url('query'),
                                                   {'query': json.dumps(query_wpseo)})
            if options_data and options_data['data']:
                option_value = php_unserialize(options_data['data'][0]['option_value'])
                if 'product_cat' in option_value:
                    option_value['product_cat'] = dict()
                data_set = {
                    'option_value': php_serialize(option_value)
                }
                where = {
                    'option_id': options_data['data'][0]['option_id'],
                    'option_name': 'wpseo_taxonomy_meta'
                }
                update_query = self.create_update_query_connector('options', data_set, where)
                wpseo_taxonomy_clear = self.import_data_connector(update_query, 'category')
        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    def _check_categories_exists(self):
        all_collections_query = {
            'type': 'select',
            'query': "SELECT term_taxonomy_id FROM `_DBPRF_term_taxonomy` WHERE taxonomy = 'product_cat' OR taxonomy = 'post_cat' LIMIT 1"
        }
        categories = self.get_connector_data(self.get_connector_url('query'),
                                             {'query': json.dumps(all_collections_query)})
        return True if categories['data'] else False

    def _check_product_exists(self):
        all_collections_query = {
            'type': 'select',
            'query': "SELECT ID FROM `_DBPRF_posts` WHERE post_type  IN ('product', 'product_variation') LIMIT 1"
        }
        # products = self.get_connector_data(self.get_connector_url('query'),
        #                                    {'query': json.dumps(all_collections_query)})
        products = self.select_data_connector(all_collections_query, 'products')
        return True if products['data'] else False

    def _check_attributes_exists(self):
        all_collections_query = {
            'type': 'select',
            'query': "SELECT * FROM `_DBPRF_woocommerce_attribute_taxonomies` ORDER BY attribute_id LIMIT 200"
        }
        products = self.get_connector_data(self.get_connector_url('query'),
                                           {'query': json.dumps(all_collections_query)})

        return True if products['data'] else False

    def clear_target_products(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_customers',
            'msg': ''
        }
        if not self._notice['config']['products']:
            self._notice['target']['clear'] = next_clear
            return next_clear

        while self._check_product_exists():
            # clear posts(product)
            # clear meta post(product)
            all_collections_query = {
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_posts` WHERE post_type IN('product', 'product_variation')"
            }
            clear_table = self.get_connector_data(self.get_connector_url('query'),
                                                  {'query': json.dumps(all_collections_query)})
            if (not clear_table) or (clear_table['result'] != 'success') or (not clear_table['data']):
                self.log("Clear data failed. Error: Could not empty products", 'clear')
                continue
            all_meta_query = {
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_postmeta` WHERE post_id NOT IN (SELECT ID FROM _DBPRF_posts)"
            }
            clear_table = self.get_connector_data(self.get_connector_url('query'),
                                                  {'query': json.dumps(all_meta_query)})

            if (not clear_table) or (clear_table['result'] != 'success'):
                self.log("Clear data failed. Error: Could not empty products", 'clear')
                continue
        # clear attributes
        del_transient_attr_query = {
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_options` WHERE option_name = '_transient_wc_attribute_taxonomies'"
        }
        clear_table = self.get_connector_data(self.get_connector_url('query'),
                                              {'query': json.dumps(del_transient_attr_query)})
        while self._check_attributes_exists():
            product_attribute_query = {
                "type": "select",
                "query": "SELECT * FROM `_DBPRF_woocommerce_attribute_taxonomies` ORDER BY attribute_id LIMIT 200"
            }
            attributes = self.get_connector_data(self.get_connector_url('query'),
                                                 {'query': json.dumps(product_attribute_query)})
            if (attributes['data']):
                attribute_ids = duplicate_field_value_from_list(attributes['data'], 'attribute_id')
                attribute_names = duplicate_field_value_from_list(attributes['data'], 'attribute_name')
                attribute_names_condition = "('pa_" + "','pa_".join(attribute_names) + "')"
                del_transient_attr_query = {
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_woocommerce_attribute_taxonomies` WHERE attribute_id IN " + self.list_to_in_condition(
                        attribute_ids)
                }
                self.get_connector_data(self.get_connector_url('query'),
                                        {'query': json.dumps(del_transient_attr_query)})
                term_query = {
                    "type": "select",
                    "query": "SELECT * FROM `_DBPRF_term_taxonomy` tt LEFT JOIN `_DBPRF_terms` t ON tt.term_id = t.term_id "
                             "WHERE tt.taxonomy IN " + attribute_names_condition
                }
                terms = self.get_connector_data(self.get_connector_url('query'),
                                                {'query': json.dumps(term_query)})
                if (terms['data']):
                    term_ids = duplicate_field_value_from_list(terms['data'], 'term_id')
                    taxonomy_ids = duplicate_field_value_from_list(terms['data'], 'term_taxonomy_id')
                    del_transient_attr_query = {
                        'type': 'query',
                        'query': "DELETE FROM `_DBPRF_term_taxonomy` WHERE term_taxonomy_id IN " + self.list_to_in_condition(
                            taxonomy_ids)
                    }
                    self.get_connector_data(self.get_connector_url('query'),
                                            {'query': json.dumps(del_transient_attr_query)})
                    del_transient_attr_query = {
                        'type': 'query',
                        'query': "DELETE FROM `_DBPRF_terms` WHERE term_id IN " + self.list_to_in_condition(
                            term_ids)
                    }
                    self.get_connector_data(self.get_connector_url('query'),
                                            {'query': json.dumps(del_transient_attr_query)})

        if self._notice['target']['support']['wpml']:
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_icl_translations` where element_type IN ('post_product','post_product_variation'"
                })
            })
        if self._notice['config']['seo'] or self._notice['config']['seo_301']:
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query',
                    'query': "DELETE FROM `_DBPRF_lecm_rewrite` where type = 'product'"
                })
            })
        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    def clear_target_customers(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_orders',
            'msg': ''
        }
        if not self._notice['config']['customers']:
            self._notice['target']['clear'] = next_clear
            return next_clear
        del_user_query = "DELETE _DBPRF_users FROM _DBPRF_users " \
                         "LEFT JOIN _DBPRF_usermeta ON _DBPRF_users.ID = _DBPRF_usermeta.user_id " \
                         "WHERE _DBPRF_usermeta.meta_key IN ('_DBPRF_capabilities', '_DBPRF_capabilities') " \
                         "AND _DBPRF_usermeta.meta_value = 'a:1:{s:8:\"customer\";b:1;}'"
        clear_table = self.get_connector_data(self.get_connector_url('query'), {
            'query': json.dumps({
                'type': 'query', 'query': del_user_query
            })
        })

        del_user_meta_query = "DELETE _DBPRF_usermeta FROM _DBPRF_usermeta " \
                              "LEFT JOIN _DBPRF_users ON _DBPRF_usermeta.user_id = _DBPRF_users.ID WHERE _DBPRF_users.ID IS NULL"
        clear_table = self.get_connector_data(self.get_connector_url('query'), {
            'query': json.dumps({
                'type': 'query', 'query': del_user_meta_query
            })
        })

        del_customer_lookup_query = "DELETE _DBPRF_wc_customer_lookup FROM _DBPRF_wc_customer_lookup LEFT JOIN _DBPRF_users ON _DBPRF_wc_customer_lookup.user_id = _DBPRF_users.ID WHERE _DBPRF_users.ID IS NULL"
        clear_table = self.get_connector_data(self.get_connector_url('query'), {
            'query': json.dumps({
                'type': 'query', 'query': del_customer_lookup_query
            })
        })

        if (not clear_table) or (clear_table['result'] != 'success') or (not clear_table['data']):
            self.log("Clear data failed. Error: Could not empty customers ", 'clear')

        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    def _check_order_exists(self):
        all_collections_query = {
            'type': 'select',
            'query': "SELECT ID FROM `_DBPRF_posts` WHERE post_type  IN ('shop_order', 'shop_order_refund') LIMIT 1"
        }
        products = self.get_connector_data(self.get_connector_url('query'),
                                           {'query': json.dumps(all_collections_query)})

        return True if products['data'] else False

    def clear_target_orders(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_reviews',
            'msg': ''
        }
        if not self._notice['config']['orders']:
            self._notice['target']['clear'] = next_clear
            return next_clear
        while self._check_order_exists():
            # clear posts(orders)
            all_collections_query = {
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_posts` WHERE post_type IN ('shop_order', 'shop_order_refund')"
            }
            clear_table = self.get_connector_data(self.get_connector_url('query'),
                                                  {'query': json.dumps(all_collections_query)})
            if (not clear_table) or (clear_table['result'] != 'success'):
                self.log("Clear data failed. Error: Could not empty products", 'clear')
                continue
            # clear meta post(orders)
            all_meta_query = {
                'type': 'select',
                'query': "DELETE `_DBPRF_postmeta` FROM `_DBPRF_post_meta` pm LEFT JOIN `_DBPRF_posts` p ON p.ID = pm.meta_id"
                         " WHERE p.ID IS NULL"
            }
            clear_table = self.get_connector_data(self.get_connector_url('query'),
                                                  {'query': json.dumps(all_meta_query)})

            if (not clear_table) or (clear_table['result'] != 'success'):
                self.log("Clear data failed. Error: Could not empty products", 'clear')
                continue
        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    def clear_target_reviews(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_blogs',
            'msg': ''
        }
        if not self._notice['config']['reviews']:
            self._notice['target']['clear'] = next_clear
            return next_clear
        tables = [
            'commentmeta',
            'comments'
        ]
        for table in tables:
            self._notice['target']['clear']['result'] = 'process'
            self._notice['target']['clear']['function'] = 'clear_target_reviews'
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "`"
                })
            })
            if (not clear_table) or (clear_table['result'] != 'success'):
                self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
                continue
        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    def clear_target_coupons(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_pages',
            'msg': ''
        }
        self._notice['target']['clear'] = next_clear
        if not self._notice['config']['coupons']:
            return next_clear
        tables = [
            'postmeta',
            'posts'
        ]
        for table in tables:
            where = ' post_type = "shop_coupon"'
            if table == 'postmeta':
                where = ' post_id IN (SELECT ID FROM _DBPRF_posts WHERE post_type = "shop_coupon")'
            clear_table = self.get_connector_data(self.get_connector_url('query'), {
                'query': json.dumps({
                    'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "` WHERE " + where
                })
            })
            if (not clear_table) or (clear_table['result'] != 'success'):
                self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
                continue
        return next_clear

    # TODO: CATEGORY
    def prepare_categories_import(self):
        parent = super().prepare_categories_import()
        if self._notice['config']['seo'] or self._notice['config']['seo_301']:
            query = self.dict_to_create_table_sql(self.lecm_rewrite_table_construct())
            self.query_data_connector({'type': 'query', 'query': query['query']})
        return self

    def prepare_categories_export(self):
        return self

    def get_categories_main_export(self):
        id_src = self._notice['process']['categories']['id_src']
        limit = self._notice['setting']['categories']
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_term_taxonomy as tx LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id"
                     " WHERE tx.taxonomy = 'product_cat' AND tx.term_id > " + to_str(
                id_src) + " AND t.term_id IS NOT NULL ORDER BY tx.term_id ASC LIMIT " + to_str(limit)
        }
        if self._notice['src']['support']['wpml']:
            query = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_term_taxonomy tt "
                         + "LEFT JOIN _DBPRF_terms AS t ON t.term_id = tt.term_id "
                         + "LEFT JOIN _DBPRF_icl_translations il ON tt.term_taxonomy_id = il.element_id "
                         + "WHERE il.`element_type` = 'tax_product_cat' "
                         + " AND il.`source_language_code` IS NULL AND tt.taxonomy = 'product_cat' AND tt.term_id > "
                         + to_str(id_src) + " ORDER BY tt.term_id ASC LIMIT " + to_str(limit),

            }
        categories = self.select_data_connector(query, 'categories')
        if not categories or categories['result'] != 'success':
            return response_error('could not get manufacturers main to export')
        return categories

    def get_categories_ext_export(self, categories):
        url_query = self.get_connector_url('query')
        category_ids = duplicate_field_value_from_list(categories['data'], 'term_id')
        parent_ids = duplicate_field_value_from_list(categories['data'], 'parent')
        cart_version = self.convert_version(self._notice['src']['config']['version'], 2)
        taxonomy_type = 'product_cat' if not categories.get('is_blog') else 'category'
        categories_ext_queries = {
            'all_category': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_term_taxonomy as tx LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id WHERE tx.taxonomy = '" + taxonomy_type + "' AND tx.term_id > 0 "
            },
            'seo_categories': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_term_taxonomy as tx LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id "
                         "WHERE tx.taxonomy = '" + taxonomy_type + "' AND tx.term_id IN " + self.list_to_in_condition(
                    parent_ids)
            }

        }
        if cart_version > 255:
            categories_ext_queries['woocommerce_termmeta'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_termmeta WHERE term_id IN " + self.list_to_in_condition(
                    category_ids) + " AND meta_key IN ('order', 'thumbnail_id', 'display_type')"
            }
        else:
            categories_ext_queries['woocommerce_termmeta'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_woocommerce_termmeta WHERE woocommerce_term_id IN " + self.list_to_in_condition(
                    category_ids) + " AND meta_key IN ('order', 'thumbnail_id', 'display_type')"
            }
        # add wpml
        if self._notice['src']['support']['wpml']:
            categories_ext_queries['icl_translations'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_icl_translations WHERE element_type = 'tax_product_cat' and element_id IN " + self.list_to_in_condition(
                    category_ids)
            }
        categories_ext = self.get_connector_data(url_query, {
            'serialize': True,
            'query': json.dumps(categories_ext_queries)
        })

        if not categories_ext or categories_ext['result'] != 'success':
            return response_warning()

        thumb_id_list = get_list_from_list_by_field(categories_ext['data']['woocommerce_termmeta'], 'meta_key',
                                                    'thumbnail_id')
        thumbnail_ids = duplicate_field_value_from_list(thumb_id_list, 'meta_value')
        thumb_ids_query = self.list_to_in_condition(thumbnail_ids)
        categories_ext_rel_queries = {
            'post_meta': {
                'type': 'select',
                'query': "SELECT p.ID, p.post_title, pm.meta_value, p.guid FROM _DBPRF_posts AS p "
                         "LEFT JOIN _DBPRF_postmeta AS pm ON pm.post_id = p.ID AND pm.meta_key = '_wp_attached_file' WHERE p.ID IN " + thumb_ids_query
            }
        }
        if self._notice['src']['support']['wpml']:
            trids = duplicate_field_value_from_list(categories_ext['data']['icl_translations'], 'trid')
            categories_ext_rel_queries['wpml_category_lang'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_icl_translations il "
                         "LEFT JOIN _DBPRF_term_taxonomy as tx ON il.element_id = tx.term_id "
                         "LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id "
                         "WHERE il.element_type = 'tax_product_cat' and il.trid IN " + self.list_to_in_condition(trids)
            }
        # add custom
        if categories_ext_rel_queries:
            categories_ext_rel = self.select_multiple_data_connector(categories_ext_rel_queries, 'categories')
            if not categories_ext_rel or categories_ext_rel['result'] != 'success':
                return response_error()
            categories_ext = self.sync_connector_object(categories_ext, categories_ext_rel)

        return categories_ext

    def convert_category_export(self, category, categories_ext):
        category_data = self.construct_category() if not self.blog_running else self.construct_blog_category()
        # category_data = self.add_construct_default(category_data)
        parent = self.construct_category_parent() if not self.blog_running else self.construct_blog_category()
        parent['id'] = 0
        if category['parent'] and to_int(category['parent']) > 0:
            parent_data = self.get_category_parent(category['parent'])
            if parent_data['result'] == 'success' and parent_data['data']:
                parent = parent_data['data']
        category_path = img_meta = category_url = img_label = ''
        cart_version = self.convert_version(self._notice['src']['config']['version'], 2)
        if cart_version > 255:
            category_src = get_list_from_list_by_field(categories_ext['data']['woocommerce_termmeta'], 'term_id',
                                                       category['term_id'])
        else:
            category_src = get_list_from_list_by_field(categories_ext['data']['woocommerce_termmeta'],
                                                       'woocommerce_term_id', category['term_id'])

        if category_src:
            category_img_id = self.get_value_metadata(category_src, 'thumbnail_id', 0)
            img_meta = get_list_from_list_by_field(categories_ext['data']['post_meta'], 'ID', category_img_id)
            if img_meta:
                img_label = img_meta[0]['post_title']
                category_path = to_str(img_meta[0]['meta_value'])
                category_url = to_str(img_meta[0]['guid']).replace(category_path, '')

        category_data['id'] = category['term_id']
        category_data['code'] = category['slug']
        category_data['name'] = category['name']
        category_data['description'] = category['description']
        category_data['parent'] = parent
        category_data['active'] = True
        category_data['thumb_image']['label'] = img_label
        category_data['thumb_image']['url'] = category_url
        category_data['thumb_image']['path'] = category_path
        category_data['sort_order'] = 1
        category_data['created_at'] = get_current_time()
        category_data['updated_at'] = get_current_time()
        category_data['category'] = category
        category_data['categories_ext'] = categories_ext

        # todo: woo2woo
        category_data['display_type'] = self.get_value_metadata(category_src, 'display_type', '')

        if self._notice['src']['support']['wpml']:
            trid = get_row_value_from_list_by_field(categories_ext['data']['icl_translations'], 'element_id',
                                                    category['term_taxonomy_id'], 'trid')
            if trid:
                languages_data = get_list_from_list_by_field(categories_ext['data']['wpml_category_lang'], 'trid', trid)
                if languages_data:
                    for language_data in languages_data:
                        category_new_data = self.construct_category_lang()
                        category_new_data['id'] = language_data['term_id']
                        category_new_data['code'] = language_data['slug']
                        category_new_data['name'] = language_data['name']
                        category_new_data['description'] = language_data['description']
                        if to_int(language_data['term_id']) == to_int(category['term_id']):
                            category_data['language_default'] = language_data['language_code']
                        elif 'language_default' not in category_data and not language_data['source_language_code']:
                            category_data['language_default'] = language_data['language_code']
                        category_data['languages'][language_data['language_code']] = category_new_data
        else:
            category_language_data = self.construct_category_lang()
            language_id = self._notice['src']['language_default']
            category_language_data['name'] = category['name']
            category_language_data['description'] = category['description']
            category_data['languages'][language_id] = category_language_data
        query_wpseo = {
            'type': 'select',
            'query': "SELECT * FROM `_DBPRF_options` WHERE `option_name` = 'wpseo_taxonomy_meta'"
        }
        options_data = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query_wpseo)})
        if options_data and options_data['data']:
            option_value = php_unserialize(options_data['data'][0]['option_value'])
            if option_value and 'product_cat' in option_value:
                if to_int(category['term_id']) in option_value['product_cat']:
                    category_data['meta_title'] = get_value_by_key_in_dict(
                        option_value['product_cat'][to_int(category['term_id'])], 'wpseo_title', '')
                    category_data['meta_description'] = get_value_by_key_in_dict(
                        option_value['product_cat'][to_int(category['term_id'])], 'wpseo_desc', '')
                    category_data['meta_keyword'] = get_value_by_key_in_dict(
                        option_value['product_cat'][to_int(category['term_id'])], 'wpseo_focuskw', '')

        # if self._notice['config']['seo']:
        detect_seo = self.detect_seo()
        category_data['seo'] = getattr(self, 'categories_' + detect_seo)(category, categories_ext)
        return response_success(category_data)

    def get_category_parent(self, parent_id):
        type_map = self.TYPE_CATEGORY if not self.blog_running else self.TYPE_CATEGORY_BLOG
        category_exist = self.select_map(self._migration_id, type_map, parent_id)
        if category_exist:
            return response_success({
                'id': parent_id,
                'code': ''
            })
        taxonomy_type = 'product_cat' if not self.blog_running else 'category'
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_term_taxonomy as tx LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id "
                     "WHERE tx.taxonomy = '" + taxonomy_type + "' AND tx.term_id = " + to_str(parent_id)
        }
        if self._notice['src']['support']['wpml']:
            query = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_term_taxonomy tt LEFT JOIN _DBPRF_terms AS t ON t.term_id = tt.term_id "
                         "LEFT JOIN _DBPRF_icl_translations il ON tt.term_taxonomy_id = il.element_id "
                         "WHERE il.`element_type` = 'tax_product_cat' AND il.`source_language_code` IS NULL AND tt.taxonomy = '" + taxonomy_type + "' and tt.term_id = " + to_str(
                    parent_id),

            }
        categories = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        if not categories or categories['result'] != 'success':
            return response_error('could not get category parent to export')
        if categories and categories['data']:
            category = categories['data'][0]
            categories_ext = self.get_categories_ext_export(categories)
            category_convert = self.convert_category_export(category, categories_ext)
            return category_convert
        return response_error('could not get category parent to export')

    def get_category_id_import(self, convert, category, categories_ext):
        return category['term_id']

    def check_category_import(self, convert, category, categories_ext):
        id_imported = self.get_map_field_by_src(self.TYPE_CATEGORY, convert['id'], convert['code'],
                                                lang=self._notice['target']['language_default'])
        return True if id_imported else False

    def router_category_import(self, convert, category, categories_ext):
        return response_success('category_import')

    def before_category_import(self, convert, category, categories_ext):
        return response_success()

    def category_import(self, convert, category, categories_ext):
        # resource data: categories
        self.log(convert, 'convert_category')

        # insert to terms
        terms = {
            'name': convert['name'],
            'slug': convert['code'] if convert['code'] else self.sanitize_title(convert['name']),
            'term_group': 0,
        }
        term_query = self.create_insert_query_connector('terms', terms)
        term_import_id = self.import_category_data_connector(term_query, True, convert['id'])
        if not term_import_id:
            return response_warning('terms[' + to_str(terms) + '] cannot insert !')
        self.insert_map(self.TYPE_CATEGORY, convert['id'], term_import_id)
        return response_success(term_import_id)

    def after_category_import(self, category_id, convert, category, categories_ext):
        # insert to term_taxonomy
        term_taxonomy = {
            'term_id': category_id,
            'taxonomy': 'product_cat',
            'description': convert.get('description', ''),
            'parent': 0,
            'count': len(categories_ext['data']['products_to_categories']),
        }
        term_taxonomy_query = self.create_insert_query_connector('term_taxonomy', term_taxonomy)
        term_taxonomy_import_id = self.import_category_data_connector(term_taxonomy_query, True, convert['id'])
        if not term_taxonomy_import_id:
            return response_warning('term_taxonomy[' + to_str(term_taxonomy) + '] cannot insert !')

        # insert to termmeta
        global thumbnail_id
        image_path = convert['thumb_image']['url'] + convert['thumb_image']['path']
        if image_path:
            search_image_query = {
                'type': 'select',
                'query': "SELECT p.ID FROM _DBPRF_posts p WHERE p.guid LIKE '" + image_path + "' LIMIT 1"
            }
            thumbnail_id = self.get_connector_data(self.get_connector_url('query'),
                                                   {'query': json.dumps(search_image_query)})
        meta_key = {
            'order': 0,
            'display_type': '',
            'thumbnail_id': thumbnail_id['data'][0]['ID'] if len(thumbnail_id['data']) > 0 else None
        }
        for key, value in meta_key.items():
            termmeta = {
                'term_id': category_id,
                'meta_key': key,
                'meta_value': value
            }
            termmeta_query = self.create_insert_query_connector('termmeta', termmeta)
            termmeta_import_id = self.import_data_connector(termmeta_query, self.TYPE_CATEGORY, convert['id'], True)
            if not termmeta_import_id:
                return response_warning('termmeta[' + to_str(termmeta) + '] cannot insert !')
        return response_success(None, 'Migration data from category to term complete !')

    def get_new_trid(self):
        query = {
            'type': 'select',
            'query': "SELECT max(trid) as trid FROM _DBPRF_icl_translations"
        }
        trid = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        new_trid = 1
        if trid['data']:
            new_trid = to_int(trid['data'][0]['trid']) + 1
        return new_trid

    def addition_category_import(self, convert, category, categories_ext):
        return response_success()

    # TODO: PRODUCT
    def prepare_products_import(self):
        parent = super().prepare_products_import()
        if self._notice['config']['seo'] or self._notice['config']['seo_301']:
            query = self.dict_to_create_table_sql(self.lecm_rewrite_table_construct())
            self.query_data_connector({'type': 'query', 'query': query['query']})
        if not self._notice['config']['add_new']:
            file_name = get_pub_path() + '/media/' + to_str(self._migration_id) + '/variants.csv'
            if os.path.isfile(file_name):
                os.remove(file_name)
        return self

    def prepare_products_export(self):
        return self

    def get_products_main_export(self):
        id_src = self._notice['process']['products']['id_src']
        limit = self._notice['setting']['products']
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_posts WHERE ID > "
                     + to_str(id_src)
                     + " AND post_type = 'product' AND post_status NOT IN ('inherit','auto-draft') "
                     + " ORDER BY ID ASC LIMIT "
                     + to_str(limit),
        }
        if self._notice['src']['support']['wpml']:
            query = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_posts p LEFT JOIN _DBPRF_icl_translations il ON p.ID = il.element_id "
                         + "WHERE il.`source_language_code` is NULL and il.`element_type` = 'post_product' AND p.ID > "
                         + to_str(id_src)
                         + " AND p.post_type = 'product' AND p.post_status NOT IN ('inherit','auto-draft') "
                         + " ORDER BY p.ID ASC LIMIT "
                         + to_str(limit),
            }
        # products = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        products = self.select_data_connector(query, 'products')
        if not products or products['result'] != 'success':
            return response_error()
        return products

    def get_products_ext_export(self, products):
        url_query = self.get_connector_url('query')
        product_ids = duplicate_field_value_from_list(products['data'], 'ID')
        product_id_con = self.list_to_in_condition(product_ids)
        # product_id_query = self.product_to_in_condition_seourl(product_ids)
        linked = self.product_to_in_condition_linked(product_ids)
        product_ext_queries = {
            'post_variant': {
                'type': "select",
                'query': "SELECT * FROM _DBPRF_posts WHERE post_type = 'product_variation' AND post_parent IN " + product_id_con,
            },
            'term_relationship': {
                'type': "select",
                'query': "SELECT * FROM _DBPRF_term_relationships AS tr "
                         "LEFT JOIN _DBPRF_term_taxonomy AS tx ON tx.term_taxonomy_id = tr.term_taxonomy_id "
                         "LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id "
                         "WHERE tr.object_id IN " + product_id_con,
            },
            'post_grouped': {
                'type': "select",
                'query': "SELECT * FROM _DBPRF_posts WHERE post_parent IN " + product_id_con + " AND post_type = 'product'",
            },
            'parent_link': {
                'type': "select",
                'query': "SELECT * FROM _DBPRF_postmeta WHERE meta_key IN ('_upsell_ids','_crosssell_ids') AND meta_value " + linked
            },
        }
        if self._notice['src']['support']['wpml']:
            product_ext_queries['icl_translations'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_icl_translations WHERE element_type = 'post_product' and element_id IN " + product_id_con
            }

        products_ext = self.select_multiple_data_connector(product_ext_queries, 'products')
        if (not products_ext) or products_ext['result'] != 'success':
            return response_error()

        pro_child_ids = duplicate_field_value_from_list(products_ext['data']['post_variant'], 'ID')
        all_product_ids = self.list_to_in_condition(list(set(pro_child_ids + product_ids)))
        variant_id_query = self.list_to_in_condition(pro_child_ids)
        taxonomy_duplicate = duplicate_field_value_from_list(products_ext['data']['term_relationship'], 'taxonomy')
        attrs_taxonomy = self.get_list_from_list_by_field_as_first_key(taxonomy_duplicate, '', 'pa_')
        attrs_name = list()
        for attr_taxonomy in attrs_taxonomy:
            attrs_name.append(self.substr_replace(attr_taxonomy, '', 0, 3))

        attr_name_query = self.list_to_in_condition(attrs_name)
        attr_values = duplicate_field_value_from_list(products_ext['data']['term_relationship'], 'term_id')
        attr_values_query = self.list_to_in_condition(attr_values)
        product_ext_rel_queries = {
            'post_meta': {
                'type': "select",
                'query': "SELECT * FROM _DBPRF_postmeta WHERE post_id IN  " + all_product_ids,
            },
            'woocommerce_attribute_taxonomies': {
                'type': "select",
                'query': "SELECT * FROM _DBPRF_woocommerce_attribute_taxonomies WHERE attribute_name IN " + attr_name_query,
            },
            'variation_term_relationship': {
                'type': "select",
                'query': "SELECT * FROM _DBPRF_term_relationships AS tr "
                         "LEFT JOIN _DBPRF_term_taxonomy AS tx ON tx.term_taxonomy_id = tr.term_taxonomy_id "
                         "LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id "
                         "WHERE tr.object_id IN " + variant_id_query,
            },
            'term_attribute': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_terms WHERE term_id IN " + attr_values_query,
            }
        }
        if self._notice['src']['support']['wpml']:
            trids = duplicate_field_value_from_list(products_ext['data']['icl_translations'], 'trid')
            product_ext_rel_queries['wpml_product_lang'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_icl_translations il "
                         "LEFT JOIN _DBPRF_posts as p ON il.element_id = p.ID "
                         "WHERE il.element_type = 'post_product' and il.trid IN " + self.list_to_in_condition(trids)
            }

            product_ext_rel_queries['wpml_product_meta'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_postmeta WHERE post_id IN (SELECT element_id FROM _DBPRF_icl_translations WHERE element_type = 'post_product' and trid IN " + self.list_to_in_condition(
                    trids) + ")"
            }

            product_ext_rel_queries['wpml_term_relationship'] = {
                'type': "select",
                'query': "SELECT * FROM _DBPRF_term_relationships AS tr "
                         "LEFT JOIN _DBPRF_term_taxonomy AS tx ON tx.term_taxonomy_id = tr.term_taxonomy_id "
                         "LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id WHERE tr.object_id IN (SELECT element_id FROM _DBPRF_icl_translations WHERE element_type = 'post_product' and trid IN " + self.list_to_in_condition(
                    trids) + ")",
            }

            product_ext_rel_queries['attributes_icl_translations'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_icl_translations il "
                         "LEFT JOIN _DBPRF_term_taxonomy as tx ON il.element_id = tx.term_id "
                         "LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id "
                         "WHERE il.element_type IN " + self.wpml_attributes_to_in_condition(
                    attrs_taxonomy)
            }

        products_ext_rel = self.select_multiple_data_connector(product_ext_rel_queries, 'products')
        if (not products_ext_rel) or products_ext_rel['result'] != 'success':
            return response_error()

        thumbnail_id_list = get_list_from_list_by_field(products_ext_rel['data']['post_meta'], 'meta_key',
                                                        '_thumbnail_id')
        thumbnail_ids = duplicate_field_value_from_list(thumbnail_id_list, 'meta_value')
        gallery_ids = gallery_ids_src = list()
        gallery_list = get_list_from_list_by_field(products_ext_rel['data']['post_meta'], 'meta_key',
                                                   '_product_image_gallery')
        if gallery_list:
            for gallery in gallery_list:
                if 'meta_value' in gallery and gallery['meta_value']:
                    images_ids = gallery['meta_value'].split(',')
                    if images_ids:
                        gallery_ids = list(set(gallery_ids + images_ids))

        for id in gallery_ids:
            if id != '':
                gallery_ids_src.append(id)

        all_images_ids = list(set(thumbnail_ids + gallery_ids_src))
        all_images_ids_query = self.list_to_in_condition(all_images_ids)
        product_ext_rel_third_queries = {
            'image': {
                'type': 'select',
                'query': "SELECT p.ID, p.post_title, pm.meta_value, p.guid FROM _DBPRF_posts AS p "
                         "LEFT JOIN _DBPRF_postmeta AS pm ON pm.post_id = p.ID AND pm.meta_key = '_wp_attached_file' "
                         "WHERE p.ID IN " + all_images_ids_query,
            }
        }
        products_ext_third = self.get_connector_data(url_query, {
            'serialize': True, 'query': json.dumps(product_ext_rel_third_queries)
        })
        if (not products_ext_third) or products_ext_third['result'] != 'success':
            return response_error()

        products_ext1 = self.sync_connector_object(products_ext_rel, products_ext_third)
        products_ext = self.sync_connector_object(products_ext, products_ext1)
        return products_ext

    def convert_product_export(self, product, products_ext):
        product_meta = get_list_from_list_by_field(products_ext['data']['post_meta'], 'post_id', product['ID'])
        product_data = self.construct_product()
        product_data = self.add_construct_default(product_data)
        product_data['id'] = product['ID']
        product_data['code'] = product['post_name']
        product_data['sku'] = self.get_value_metadata(product_meta, '_sku')

        # todo: get type prd virtual
        product_type = get_row_value_from_list_by_field(product_meta, 'meta_key', '_virtual', 'meta_value')
        if product_type == 'yes':
            product_data['type'] = 'virtual'

        product_price = ''
        if to_decimal(self.get_value_metadata(product_meta, '_regular_price', 0.0000)) > 0:
            product_price = self.get_value_metadata(product_meta, '_regular_price', 0.0000)
        else:
            product_price = self.get_value_metadata(product_meta, '_price', 0.0000)

        if product_price == '' or product_price == self.get_value_metadata(product_meta, '_min_variation_sale_price',
                                                                           0.0000):
            product_price = self.get_value_metadata(product_meta, '_min_variation_regular_price', 0.0000)

        if product_price == '' or not product_price:
            product_price = 0

        product_data['price'] = product_price
        product_data['weight'] = self.get_value_metadata(product_meta, '_weight', 0.0000)
        product_data['length'] = self.get_value_metadata(product_meta, '_length', 0.0000)
        product_data['width'] = self.get_value_metadata(product_meta, '_width', 0.0000)
        product_data['height'] = self.get_value_metadata(product_meta, '_height', 0.0000)
        product_data['status'] = True if product['post_status'] == "publish" else False
        product_data['manage_stock'] = True if self.get_value_metadata(product_meta, '_manage_stock',
                                                                       'no') == "yes" else False
        if self.is_woo2woo():
            product_data['is_in_stock'] = self.get_value_metadata(product_meta, '_stock_status', 'instock')
            product_data['sold_individually'] = self.get_value_metadata(product_meta, '_sold_individually', '')
            product_data['purchase_note'] = self.get_value_metadata(product_meta, '_purchase_note', '')
        else:
            product_data['is_in_stock'] = True if self.get_value_metadata(product_meta, '_stock_status',
                                                                          'instock') == "instock" else False
        product_data['qty'] = to_int(to_decimal(self.get_value_metadata(product_meta, '_stock', 0))) if to_decimal(
            self.get_value_metadata(product_meta, '_stock', 0)) > 0 else 0
        product_data['created_at'] = convert_format_time(product['post_date'])
        product_data['updated_at'] = convert_format_time(product['post_modified'])
        product_data['name'] = product['post_title']
        product_data['description'] = product['post_content']
        product_data['short_description'] = product['post_excerpt']
        product_data['menu_order'] = product['menu_order']
        product_data['sort_order'] = product['menu_order']
        product_data['backorders'] = self.get_value_metadata(product_meta, '_backorders', 'no')
        product_data['meta_description'] = self.get_value_metadata(product_meta, '_yoast_wpseo_metadesc', '')
        product_data['meta_title'] = self.get_value_metadata(product_meta, '_yoast_wpseo_title', '')
        if product_data['meta_title']:
            product_data['meta_title'] = product_data['meta_title'].replace('%%title%%', product_data['name']).replace(
                '%%page%%', '').replace('%%sep%%', '-').replace('%%sitename%%', '')
        # image_
        thumbnail_id = self.get_value_metadata(product_meta, '_thumbnail_id', 0)
        if thumbnail_id:
            thumbnail_src = get_list_from_list_by_field(products_ext['data']['image'], 'ID', thumbnail_id)
            if thumbnail_src:
                product_data['thumb_image']['label'] = thumbnail_src[0]['post_title']
                product_data['thumb_image']['url'] = self._notice['src']['cart_url'].rstrip(
                    '/') + '/wp-content/uploads/' + to_str(thumbnail_src[0]['meta_value']).lstrip('/')
                product_data['thumb_image']['url'] = to_str(product_data['thumb_image']['url']).replace(
                    'uploads/uploads', 'uploads')
        gallery_ids = self.get_value_metadata(product_meta, '_product_image_gallery', '')
        if gallery_ids:
            gallery_ids = gallery_ids.split(',')
            for gallery_id in gallery_ids:
                image_gallery_src = get_list_from_list_by_field(products_ext['data']['image'], 'ID', gallery_id)
                product_image_data = self.construct_product_image()
                if image_gallery_src:
                    product_image_data['label'] = image_gallery_src[0]['post_title']
                    product_image_data['url'] = self._notice['src']['cart_url'].rstrip('/') + '/wp-content/uploads/' + \
                                                image_gallery_src[0]['meta_value'].lstrip('/')
                    product_image_data['url'] = to_str(product_image_data['url']).replace('uploads/uploads', 'uploads')
                    product_data['images'].append(product_image_data)

        sale_price = self.get_value_metadata(product_meta, '_sale_price', '')
        if sale_price != '':
            product_data['special_price']['price'] = to_decimal(sale_price)
            start_date = self.get_value_metadata(product_meta, '_sale_price_dates_from', '')
            if start_date:
                product_data['special_price']['start_date'] = convert_format_time(start_date)
            end_date = self.get_value_metadata(product_meta, '_sale_price_dates_to', '')
            if end_date:
                product_data['special_price']['end_date'] = convert_format_time(
                    self.get_value_metadata(product_meta, '_sale_price_dates_to', ''))
        else:
            product_data['special_price']['price'] = self.get_value_metadata(product_meta, '_min_variation_sale_price',
                                                                             0.0000)

        if not product_data['special_price']['price']:
            product_data['special_price']['price'] = 0

        crosssell_ids = self.get_value_metadata(product_meta, '_crosssell_ids', '')
        if crosssell_ids:
            crosssell_ids_data = php_unserialize(crosssell_ids)
            if crosssell_ids_data:
                for crosssell_id in crosssell_ids_data:
                    relation = self.construct_product_relation()
                    relation['id'] = crosssell_id
                    relation['type'] = self.PRODUCT_CROSS
                    product_data['relate']['children'].append(relation)
        parent_crosssell_list = get_list_from_list_by_field(products_ext['data']['parent_link'], 'meta_key',
                                                            '_crosssell_ids')
        if parent_crosssell_list:
            for parent_crosssell in parent_crosssell_list:
                if parent_crosssell['meta_value'].find(':' + to_str(product['ID']) + ';') != -1:
                    relation = self.construct_product_relation()
                    relation['id'] = parent_crosssell['post_id']
                    relation['type'] = self.PRODUCT_CROSS
                    product_data['relate']['parent'].append(relation)
        upsell_ids = self.get_value_metadata(product_meta, '_upsell_ids', '')
        if upsell_ids:
            upsell_ids_data = php_unserialize(upsell_ids)
            if upsell_ids_data:
                for upsell_id in upsell_ids_data:
                    relation = self.construct_product_relation()
                    relation['id'] = upsell_id
                    relation['type'] = self.PRODUCT_UPSELL
                    product_data['relate']['children'].append(relation)
        parent_upsell_list = get_list_from_list_by_field(products_ext['data']['parent_link'], 'meta_key', '_upsell_ids')
        if parent_upsell_list:
            for parent_upsell in parent_upsell_list:
                if parent_upsell['meta_value'].find(':' + to_str(product['ID']) + ';') != -1:
                    relation = self.construct_product_relation()
                    relation['id'] = parent_upsell['post_id']
                    relation['type'] = self.PRODUCT_UPSELL
                    product_data['relate']['parent'].append(relation)
        product_data['tax']['code'] = self.get_value_metadata(product_meta, '_tax_class',
                                                              'standard') if self.get_value_metadata(product_meta,
                                                                                                     '_tax_status',
                                                                                                     'taxable') != 'none' else None
        product_data['tax']['status'] = self.get_value_metadata(product_meta, '_tax_status', 'taxable')
        # category product
        term_relationship = get_list_from_list_by_field(products_ext['data']['term_relationship'], 'object_id',
                                                        product['ID'])
        category_src = get_list_from_list_by_field(term_relationship, 'taxonomy', 'product_cat')
        if category_src:
            for product_category in category_src:
                product_category_data = self.construct_product_category()
                product_category_data['id'] = product_category['term_id']
                product_category_data['code'] = product_category['slug']
                product_data['categories'].append(product_category_data)
        if self._notice['src']['support']['manufacturers']:
            manu_src = get_row_from_list_by_field(term_relationship, 'taxonomy', 'product_brand')
            if not manu_src:
                manu_src = get_row_from_list_by_field(term_relationship, 'taxonomy', 'pwb-brand')
            if manu_src:
                product_manufacturer_data = dict()
                product_manufacturer_data['id'] = manu_src['term_id']
                product_manufacturer_data['name'] = manu_src['name']
                product_manufacturer_data['code'] = manu_src['slug']
                product_data['manufacturer'] = product_manufacturer_data
        # tags
        product_tags = get_list_from_list_by_field(term_relationship, 'taxonomy', 'product_tag')
        if product_tags:
            tags = list()
            for product_tag in product_tags:
                tags.append(product_tag['name'])
            if tags:
                product_data['tags'] = ','.join(tags)

        # if self._notice['config']['seo']:
        detect_seo = self.detect_seo()
        product_data['seo'] = getattr(self, 'products_' + detect_seo)(product, products_ext)
        # TODO: convert product languages
        if self._notice['src']['support']['wpml']:
            trid = get_row_value_from_list_by_field(products_ext['data']['icl_translations'], 'element_id',
                                                    product['ID'], 'trid')
            if trid:
                language_datas = get_list_from_list_by_field(products_ext['data']['wpml_product_lang'], 'trid', trid)
                if language_datas:
                    for language_data in language_datas:
                        if not language_data['post_title']:
                            continue
                        meta_language_datas = get_list_from_list_by_field(products_ext['data']['wpml_product_meta'],
                                                                          'post_id', language_data['ID'])
                        term_relationship_language = get_list_from_list_by_field(
                            products_ext['data']['wpml_term_relationship'], 'object_id', language_data['ID'])
                        product_new_data = self.construct_product_lang()
                        product_new_data['name'] = language_data['post_title']
                        product_new_data['code'] = language_data['post_name']
                        product_new_data['description'] = language_data['post_content']
                        product_new_data['short_description'] = language_data['post_excerpt']
                        product_new_data['meta_description'] = self.get_value_metadata(meta_language_datas,
                                                                                       '_yoast_wpseo_metadesc', '')
                        product_new_data['meta_title'] = self.get_value_metadata(meta_language_datas,
                                                                                 '_yoast_wpseo_title', '')
                        if product_new_data['meta_title']:
                            product_new_data['meta_title'] = product_new_data['meta_title'].replace('%%title%%',
                                                                                                    product_new_data[
                                                                                                        'name']).replace(
                                '%%page%%', '').replace('%%sep%%', '-').replace('%%sitename%%', '')
                        wpml_product_tags = get_list_from_list_by_field(term_relationship_language, 'taxonomy',
                                                                        'product_tag')
                        if wpml_product_tags:
                            wpml_tags = list()
                            for wpml_product_tag in wpml_product_tags:
                                wpml_tags.append(wpml_product_tag['name'])
                            if wpml_tags:
                                product_new_data['tags'] = ','.join(wpml_tags)

                        if not language_data['source_language_code']:
                            product_data['language_default'] = language_data['language_code']
                        product_data['languages'][language_data['language_code']] = product_new_data
        else:
            product_language_data = self.construct_product_lang()
            product_language_data['name'] = product['post_title']
            product_language_data['description'] = product['post_content']
            product_language_data['short_description'] = product['post_excerpt']
            language_id = self._notice['src']['language_default']
            product_data['languages'][language_id] = product_language_data
        # attribute product
        product_child_src = get_list_from_list_by_field(products_ext['data']['post_variant'], 'post_parent',
                                                        product['ID'])
        # todo: attribute
        product_attribute = get_row_value_from_list_by_field(product_meta, 'meta_key', '_product_attributes',
                                                             'meta_value')
        product_attribute = php_unserialize(product_attribute)
        if isinstance(product_attribute, str):
            product_attribute = php_unserialize(product_attribute)
        src_option_values = get_list_from_list_by_field(products_ext['data']['term_relationship'], 'object_id',
                                                        product['ID'])
        attribute_variants = list()
        if product_attribute:
            for attribute_key, attribute in product_attribute.items():
                if to_int(attribute.get('is_taxonomy')) > 0:
                    woo_attribute = get_row_from_list_by_field(products_ext['data']['woocommerce_attribute_taxonomies'],
                                                               'attribute_name',
                                                               to_str(attribute_key).replace('pa_', ''))
                    if not woo_attribute:
                        woo_attribute = get_row_from_list_by_field(
                            products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name',
                            to_str(attribute['name']).replace('pa_', ''))
                else:
                    woo_attribute = None
                if woo_attribute:
                    # attributes
                    attribute_data = self.construct_product_attribute()
                    attribute_data['option_id'] = woo_attribute['attribute_id']
                    option_code = to_str(woo_attribute['attribute_name']).lower()
                    attribute_data['option_code'] = option_code.strip()
                    attribute_data['option_type'] = woo_attribute['attribute_type']
                    attribute_data['option_name'] = woo_attribute['attribute_label']
                    attribute_data['option_group'] = woo_attribute['attribute_orderby']
                    attribute_data['is_visible'] = attribute.get('is_visible', 'visible')
                    attribute_data['is_variation'] = True if to_int(attribute.get('is_variation')) == 1 else False
                    attribute_data['is_taxonomy'] = True if to_int(attribute.get('is_taxonomy')) == 1 else False
                    # attribute language
                    attribute_language_data = self.construct_product_option_lang()
                    attribute_language_data['option_name'] = woo_attribute['attribute_label']
                    language_id = self._notice['src']['language_default']
                    attribute_data['option_languages'][language_id] = attribute_language_data
                    # attribute values
                    tmp_values = list()
                    desc = list()
                    for option_value in src_option_values:
                        attribute_name = 'pa_' + to_str(woo_attribute['attribute_name']).lower()
                        if 'taxonomy' in option_value:
                            if option_value['taxonomy'] == attribute_name:
                                woo_term_values = get_list_from_list_by_field(
                                    products_ext['data']['term_attribute'], 'term_id', option_value['term_id'])
                                if woo_term_values:
                                    for woo_term in woo_term_values:
                                        attribute_value = woo_term['name']
                                        if woo_attribute['attribute_type'] in ['select', 'alg_wc_civs_image']:
                                            option_values = to_str(woo_term['name']).split('|')
                                            if option_values and to_len(option_values) > 1:
                                                attribute_value = ';'.join(option_values)
                                            tmp_values.append(attribute_value)
                                        desc.append(option_value['description'])
                    values = list(map(lambda x: x.strip(), tmp_values))
                    if values and to_len(values) > 1:
                        attribute_data['option_type'] = self.OPTION_MULTISELECT
                    attribute_data['option_value_name'] = ';'.join(values)
                    attribute_data['option_value_description'] = ';'.join(desc)
                    attribute_data['option_value_languages'][self._notice['src']['language_default']] = {
                        'option_value_name': ';'.join(values)
                    }
                    if (to_int(attribute.get('is_variation')) == 1 or to_str(
                            attribute.get('variation')) == 'yes') and not self.is_woo2woo():
                        attribute_variants.append(attribute_data)
                    else:
                        product_data['attributes'].append(attribute_data)
                else:
                    if ('is_visible' in attribute and to_int(attribute['is_visible']) == 1) or (
                            'visible' in attribute and attribute['visible'] == 'yes'):
                        attribute_data = self.construct_product_attribute()
                        attribute_data['option_id'] = None
                        option_code = to_str(attribute['name']).lower()
                        attribute_data['option_code'] = option_code.lower().strip()
                        attribute_data['option_type'] = 'text'
                        attribute_data['option_name'] = attribute['name']
                        attribute_data['option_group'] = 'menu_order'
                        attribute_data['is_visible'] = attribute.get('is_visible', 'visible')
                        attribute_data['is_variation'] = True if to_int(attribute.get('is_variation')) == 1 else False
                        # attribute language
                        attribute_language_data = self.construct_product_option_lang()
                        attribute_language_data['option_name'] = attribute['name']
                        language_id = self._notice['src']['language_default']
                        attribute_data['option_languages'][language_id] = attribute_language_data
                        # attribute values
                        attribute_value = attribute['value']
                        if attribute_value and attribute_value != '':
                            option_values = list()
                            if isinstance(attribute_value, dict):
                                for key, attr_value in attribute_value.items():
                                    option_values.append(attr_value)
                            else:
                                option_values = attribute_value.split('|')
                            if option_values and to_len(option_values) > 1:
                                attribute_data['option_type'] = 'multiselect'
                                option_values = list(map(lambda x: x.strip(), option_values))
                                attribute_value = ';'.join(option_values)

                        attribute_data['option_value_name'] = attribute_value
                        attribute_data['option_value_languages'][self._notice['src']['language_default']] = {
                            'option_value_name': attribute_value
                        }
                    # product_data['attributes'].append(attribute_data)
                    else:
                        attribute_data = self.construct_product_attribute()
                        attribute_data['option_id'] = None
                        option_code = to_str(attribute['name']).lower()
                        attribute_data['option_code'] = option_code.lower().strip()
                        attribute_data['option_type'] = 'text'
                        attribute_data['option_name'] = attribute['name']
                        attribute_data['option_group'] = 'menu_order'
                        attribute_data['is_visible'] = attribute.get('is_visible', 'visible')
                        attribute_data['is_variation'] = True if to_int(attribute.get('is_variation')) == 1 else False
                        # attribute language
                        attribute_language_data = self.construct_product_option_lang()
                        attribute_language_data['option_name'] = attribute['name']
                        language_id = self._notice['src']['language_default']
                        attribute_data['option_languages'][language_id] = attribute_language_data
                        # attribute values
                        option_values = attribute['value']
                        if option_values != '':
                            option_values = option_values.split('|')
                            if option_values and to_len(option_values) > 1:
                                attribute_data['option_type'] = self.OPTION_MULTISELECT
                                option_values = list(map(lambda x: x.strip(), option_values))
                            option_values = ';'.join(option_values)
                        attribute_data['option_value_name'] = option_values
                        attribute_data['option_value_languages'][self._notice['src']['language_default']] = {
                            'option_value_name': option_values
                        }
                    if (to_int(attribute.get('is_variation')) == 1 or to_str(
                            attribute.get('variation')) == 'yes') and not self.is_woo2woo():
                        attribute_variants.append(attribute_data)
                    else:
                        product_data['attributes'].append(attribute_data)

        # end

        # todo: plugin add-ons
        if self._notice['src']['support'].get('addons') and not self.is_woo2woo():
            product_addons = get_row_value_from_list_by_field(product_meta, 'meta_key', '_product_addons', 'meta_value')
            product_addons = php_unserialize(product_addons)
            if product_addons and to_len(product_addons) > 0:
                for product_addon in product_addons:
                    if not product_addon.get('options') or to_len(product_addon['options']) == 0:
                        continue
                    if product_addon.get('type') == 'radiobutton':
                        option_type = self.OPTION_RADIO
                    else:
                        option_type = self.OPTION_SELECT
                    product_option = self.construct_product_option()
                    product_option['code'] = self.convert_attribute_code(product_addon.get('name'))
                    product_option['option_code'] = self.convert_attribute_code(product_addon.get('name'))
                    product_option['option_name'] = product_addon.get('name')
                    product_option['type'] = option_type
                    product_option['position'] = product_addon.get('position')
                    product_option['required'] = True if product_addon.get('required') and to_int(
                        product_addon.get('required')) == 1 else False
                    product_addon_options = list()
                    if isinstance(product_addon.get('options'), dict):
                        for key, product_addon_value in product_addon['options'].items():
                            product_addon_options.append(product_addon_value)
                    else:
                        product_addon_options = product_addon.get('options')
                    for product_addon_value in product_addon_options:
                        product_option_value = self.construct_product_option_value()
                        product_option_value['code'] = self.convert_attribute_code(product_addon_value.get('label'))
                        product_option_value['option_value_code'] = self.convert_attribute_code(
                            product_addon_value.get('label'))
                        product_option_value['option_value_name'] = product_addon_value.get('label')
                        product_option_value['option_value_price'] = product_addon_value.get('price')
                        if 'Color' in product_addon.get('name', '') or 'Colour' in product_addon.get('name', ''):
                            if 'RNBP' in product_addon_value.get('label', ''):
                                product_option_value['thumb_image']['path'] = self.convert_attribute_code(
                                    to_str(product_addon_value.get('label')).replace(' (RNBP)', '')) + '.jpg'
                                product_option_value['thumb_image']['url'] = self._notice['src']['cart_url'].rstrip(
                                    '/') + '/assets/blind-images/rnbp/'
                        product_option['values'].append(product_option_value)
                    product_data['options'].append(product_option)

        # todo: downloadable
        product_downloadables = get_row_value_from_list_by_field(product_meta, 'meta_key', '_downloadable_files',
                                                                 'meta_value')
        product_downloadables = php_unserialize(product_downloadables)
        if product_downloadables:
            product_data['type'] = self.PRODUCT_DOWNLOAD
            for key, product_downloadable in product_downloadables.items():
                download_data = self.construct_product_downloadable()
                download_data['limit'] = get_row_value_from_list_by_field(product_meta, 'meta_key', '_download_limit',
                                                                          'meta_value')
                download_data['max_day'] = get_row_value_from_list_by_field(product_meta, 'meta_key',
                                                                            '_download_expiry', 'meta_value')
                name_file = to_str(product_downloadable['file']).split('/') if product_downloadable.get(
                    'file') else None
                if self._notice['src']['cart_url'] in product_downloadable['file'] and name_file:
                    download_data['name'] = to_str(product_downloadable['file']).split('/')
                    download_data['path'] = 'woocommerce/' + to_str(name_file[to_len(name_file) - 1]).lower()
                else:
                    download_data['name'] = product_downloadable['name']
                    download_data['path'] = product_downloadable['file']
                # Thieu max_day,limit
                product_data['downloadable'].append(download_data)

        # todo: group product
        child_group_product = self.get_value_metadata(product_meta, '_children', '')
        if child_group_product:
            child_group_product = php_unserialize(child_group_product)
            if child_group_product and to_len(child_group_product) > 0:
                for child_group_product_id in child_group_product:
                    product_data['group_child_ids'].append({
                        'id': child_group_product_id
                    })
                product_data['type'] = self.PRODUCT_GROUP

        # todo: child product
        product_child_src = get_list_from_list_by_field(products_ext['data']['post_variant'], 'post_parent',
                                                        product['ID'])
        all_child = dict()
        child_attributes = dict()
        if product_child_src:
            product_data['type'] = self.PRODUCT_CONFIG
            for product_child in product_child_src:
                child_attributes[product_child['ID']] = dict()
                child_data = self.construct_product_child()
                child_data = self.add_construct_default(child_data)
                child_meta = get_list_from_list_by_field(products_ext['data']['post_meta'], 'post_id',
                                                         product_child['ID'])
                child_data['id'] = product_child['ID']
                child_data['sku'] = self.get_value_metadata(child_meta, '_sku', '') if self.get_value_metadata(
                    child_meta, '_sku', '') else self.get_value_metadata(product_meta, '_sku', '')
                child_data['code'] = product_child['post_name']
                child_product_price = ''
                if self.get_value_metadata(child_meta, '_regular_price', ''):
                    child_product_price = self.get_value_metadata(child_meta, '_regular_price')
                else:
                    if self.get_value_metadata(child_meta, '_price', ''):
                        child_product_price = self.get_value_metadata(child_meta, '_price', 0.0000)
                    else:
                        child_product_price = 0
                if child_product_price == '' or not child_product_price:
                    child_product_price = 0

                child_data['price'] = child_product_price
                child_data['weight'] = self.get_value_metadata(child_meta, '_weight') if self.get_value_metadata(
                    child_meta, '_weight') else product_data['weight']
                child_data['length'] = self.get_value_metadata(child_meta, '_length') if self.get_value_metadata(
                    child_meta, '_length') else product_data['length']
                child_data['width'] = self.get_value_metadata(child_meta, '_width') if self.get_value_metadata(
                    child_meta, '_width') else product_data['width']
                child_data['height'] = self.get_value_metadata(child_meta, '_height') if self.get_value_metadata(
                    child_meta, '_height') else product_data['height']
                child_data['status'] = True if product_child['post_status'] == "publish" else False
                child_data['manage_stock'] = True if self.get_value_metadata(child_meta,
                                                                             '_manage_stock') == 'yes' else False
                if self.is_woo2woo():
                    child_data['is_in_stock'] = self.get_value_metadata(child_meta, '_stock_status', 'instock')
                    child_data['sold_individually'] = self.get_value_metadata(child_meta, '_sold_individually', '')
                    child_data['purchase_note'] = self.get_value_metadata(child_meta, '_purchase_note', '')
                else:
                    child_data['is_in_stock'] = True if self.get_value_metadata(child_meta, '_stock_status',
                                                                                'instock') == "instock" else False
                child_data['qty'] = to_int(
                    to_decimal(self.get_value_metadata(child_meta, '_stock'))) if self.get_value_metadata(child_meta,
                                                                                                          '_stock') else 0
                child_data['created_at'] = convert_format_time(product_child['post_date'])
                child_data['updated_at'] = convert_format_time(product_child['post_modified'])
                child_data['name'] = product_child['post_title']
                child_data['description'] = self.get_value_metadata(child_meta, '_variation_description')
                child_data['tax']['code'] = self.get_value_metadata(child_meta, '_tax_class', 'standard')
                child_data['short_description'] = ''
                # image_
                thumbnail_id = self.get_value_metadata(child_meta, '_thumbnail_id')
                if thumbnail_id:
                    thumbnail_src = get_list_from_list_by_field(products_ext['data']['image'], 'ID', thumbnail_id)
                    if thumbnail_src:
                        child_data['thumb_image']['label'] = thumbnail_src[0]['post_title']
                        child_data['thumb_image']['path'] = thumbnail_src[0]['meta_value']
                        child_data['thumb_image']['url'] = to_str(thumbnail_src[0]['guid']).replace(
                            thumbnail_src[0]['meta_value'], '')

                sale_price = self.get_value_metadata(child_meta, '_sale_price')
                if sale_price != '':
                    child_data['special_price']['price'] = sale_price
                    child_data['special_price']['start_date'] = convert_format_time(
                        self.get_value_metadata(child_meta, '_sale_price_dates_from'))
                    child_data['special_price']['end_date'] = convert_format_time(
                        self.get_value_metadata(child_meta, '_sale_price_dates_to'))

                child_product_language_data = self.construct_product_lang()
                child_product_language_data['name'] = product_child['post_title']
                child_product_language_data['description'] = self.get_value_metadata(child_meta,
                                                                                     '_variation_description')
                child_product_language_data['short_description'] = product_child['post_excerpt']
                language_id = self._notice['src']['language_default']
                child_data['languages'][language_id] = child_product_language_data

                attr_child = self.get_list_from_list_by_field_as_first_key(child_meta, 'meta_key', 'attribute_')
                child_data['options'] = list()
                child_data['attributes'] = list()
                for attribute in attr_child:
                    # attribute
                    attribute_child_data = self.construct_product_attribute()
                    attr_name = to_str(attribute['meta_key']).replace('attribute_', '')
                    element_type = 'tax_' + attr_name
                    attr_name = attr_name.replace('pa_', '')
                    attr_name = attr_name.strip()
                    option_id = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_id')
                    attribute_child_data['option_id'] = option_id if option_id else ''
                    option_name = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_label')

                    attribute_child_data['option_name'] = option_name if option_name else attr_name
                    option_code = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_name')
                    attribute_child_data['option_code'] = option_code if option_code else attr_name.lower()
                    option_type = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_type')
                    # attribute_child_data['option_type'] = option_type if option_type else 'select'
                    attribute_child_data['option_type'] = self.OPTION_SELECT
                    option_group = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_orderby')
                    attribute_child_data['option_group'] = option_group if option_group else 'menu_order'
                    # attribute language
                    child_attribute_language_data = self.construct_product_option_lang()
                    child_attribute_language_data['option_name'] = attribute_child_data['option_name']
                    language_id = self._notice['src']['language_default']
                    attribute_child_data['option_languages'][language_id] = child_attribute_language_data
                    # values
                    attribute_child_data['option_value_id'] = get_row_value_from_list_by_field(
                        products_ext['data']['term_attribute'], 'slug', attribute['meta_value'], 'term_id')
                    option_value_name = get_row_value_from_list_by_field(products_ext['data']['term_attribute'], 'slug',
                                                                         attribute['meta_value'], 'name')
                    attribute_child_data['option_value_name'] = get_row_value_from_list_by_field(
                        products_ext['data']['term_attribute'], 'slug', attribute['meta_value'],
                        'name') if get_row_value_from_list_by_field(products_ext['data']['term_attribute'], 'slug',
                                                                    attribute['meta_value'], 'name') else attribute[
                        'meta_value']
                    attribute_child_data['option_value_code'] = to_str(attribute['meta_value']).lower()
                    attribute_child_data['option_value_description'] = get_row_value_from_list_by_field(
                        products_ext['data']['term_relationship'], 'slug', attribute['meta_value'],
                        'description') if get_row_value_from_list_by_field(products_ext['data']['term_relationship'],
                                                                           'slug', attribute['meta_value'],
                                                                           'description') else ''
                    language_id = self._notice['src']['language_default']
                    child_attribute_value_language_data = self.construct_product_option_value_lang()
                    child_attribute_value_language_data['option_value_name'] = get_row_value_from_list_by_field(
                        products_ext['data']['term_attribute'], 'slug', attribute['meta_value'],
                        'name') if get_row_value_from_list_by_field(products_ext['data']['term_attribute'], 'slug',
                                                                    attribute['meta_value'], 'name') else attribute[
                        'meta_value']
                    attribute_child_data['option_value_languages'][language_id] = child_attribute_value_language_data
                    child_data['attributes'].append(attribute_child_data)
                    # options
                    child_option_data = self.construct_product_option()
                    child_option_data['id'] = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_id')
                    child_option_data['code'] = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_name') if get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_name') else attr_name.lower()
                    child_option_data['option_name'] = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_label') if get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_label') else attr_name
                    child_option_data['option_code'] = child_option_data['code']
                    child_option_data['option_group'] = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_orderby') if get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_orderby') else 'menu_order'
                    # child_option_data['option_type'] = self.OPTION_SELECT
                    child_option_data['option_type'] = get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_type') if get_row_value_from_list_by_field(
                        products_ext['data']['woocommerce_attribute_taxonomies'], 'attribute_name', attr_name,
                        'attribute_type') else 'select'
                    child_option_data['required'] = 1
                    # option language
                    child_option_language_data = self.construct_product_option_lang()
                    child_option_language_data['option_name'] = attr_name
                    language_id = self._notice['src']['language_default']
                    child_option_data['option_languages'][language_id] = child_option_language_data
                    # value option
                    child_option_value_data = self.construct_product_option_value()
                    child_option_value_data['id'] = get_row_value_from_list_by_field(
                        products_ext['data']['term_attribute'], 'slug', attribute['meta_value'], 'term_id')
                    child_option_value_data['code'] = attribute['meta_value']
                    child_option_value_data['option_value_code'] = attribute['meta_value']
                    child_option_value_data['option_value_name'] = get_row_value_from_list_by_field(
                        products_ext['data']['term_attribute'], 'slug', attribute['meta_value'],
                        'name') if get_row_value_from_list_by_field(products_ext['data']['term_attribute'], 'slug',
                                                                    attribute['meta_value'], 'name') else \
                        child_option_value_data['code']
                    child_option_value_data['option_value_description'] = get_row_value_from_list_by_field(
                        products_ext['data']['term_relationship'], 'slug', attribute['meta_value'],
                        'description') if get_row_value_from_list_by_field(products_ext['data']['term_relationship'],
                                                                           'slug', attribute['meta_value'],
                                                                           'name') else ''
                    # value language
                    child_option_value_language_data = self.construct_product_option_value_lang()
                    child_option_value_language_data['option_value_name'] = get_row_value_from_list_by_field(
                        products_ext['data']['term_attribute'], 'slug', attribute['meta_value'], 'name')
                    language_id = self._notice['src']['language_default']
                    child_option_value_data['option_value_languages'][language_id] = child_option_value_language_data
                    child_option_data['values'].append(child_option_value_data)
                    child_attributes[product_child['ID']][child_option_data['option_name']] = child_option_value_data[
                        'option_value_name']
                all_child[to_str(product_child['ID'])] = child_data

        # todo: bundle product - product bundle plugin: WPC Product Bundles for WooCommerce (Premium)
        if self._notice['src']['support']['product_bundle']:
            product_data['bundle_selection'] = list()
            product_bundles = get_row_value_from_list_by_field(product_meta, 'meta_key', 'woosb_ids', 'meta_value')
            if product_bundles:
                product_data['type'] = self.PRODUCT_BUNDLE
                product_bundle_list = to_str(product_bundles).split(',')
                if product_bundle_list and to_len(product_bundle_list) > 0:
                    for product_bundle_child in product_bundle_list:
                        product_bundle_ids = to_str(product_bundle_child).split('/')
                        if product_bundle_ids and to_len(product_bundle_ids) > 0:
                            product_bundle_data = {
                                'product_id': product_bundle_ids[0],
                                'selection_qty': product_bundle_ids[1] if to_len(product_bundle_ids) > 1 else 1
                            }
                            product_data['bundle_selection'].append(product_bundle_data)

        if self.is_woo2woo():
            product_data['children'] = list(all_child.values())
        else:
            len_child = 1
            for attribute_variant in attribute_variants:
                len_child *= to_len(attribute_variant['option_value_name'].split(';'))
            options_src = dict()
            for attribute_variant in attribute_variants:
                values = to_str(attribute_variant['option_value_name']).split(';')
                option_data = self.construct_product_option()
                option_data['id'] = attribute_variant['option_id']
                option_data['option_name'] = attribute_variant['option_name']
                option_data['option_code'] = attribute_variant['option_code']
                option_data['option_type'] = 'select'
                for value in values:
                    if len_child > self.VARIANT_LIMIT:
                        option_data_value = self.construct_product_option_value()
                        option_data_value['option_value_name'] = value
                        option_data['values'].append(option_data_value)
                    opt_val = {
                        'option_name': attribute_variant['option_name'],
                        'option_code': attribute_variant['option_code'],
                        'option_languages': attribute_variant['option_languages'],
                        'option_id': attribute_variant['option_id'],
                        'option_value_name': value,
                    }
                    if attribute_variant['option_name'] not in options_src:
                        options_src[attribute_variant['option_name']] = list()
                    options_src[attribute_variant['option_name']].append(opt_val)
                if len_child > self.VARIANT_LIMIT:
                    product_data['options'].append(option_data)
            if len_child <= self.VARIANT_LIMIT and child_attributes:
                combinations = self.combination_from_multi_dict(options_src)
                list_child = list()
                if combinations:
                    for combination in combinations:
                        if not combination:
                            continue
                        children_id = None
                        check_any = False
                        for child_id, child in child_attributes.items():
                            if self.check_sync_child(child, combination) and child_id not in list_child:
                                children_id = child_id
                                list_child.append(child_id)
                                break
                        if not children_id:
                            for child_id, child in child_attributes.items():
                                if self.check_sync_child(child, combination, True) and child_id not in list_child:
                                    children_id = child_id
                                    check_any = True
                                    break
                        if not children_id:
                            continue
                        child = copy.deepcopy(all_child[children_id])
                        child['attributes'] = list()
                        for attribute in combination:
                            attribute_data = self.construct_product_attribute()
                            attribute_data['option_name'] = attribute['option_name']
                            attribute_data['option_code'] = attribute['option_code']
                            attribute_data['option_languages'] = attribute['option_languages']
                            attribute_data['option_id'] = attribute['option_id']
                            attribute_data['option_value_name'] = attribute['option_value_name']
                            child['attributes'].append(attribute_data)
                        product_data['children'].append(child)
            else:
                if attribute_variants:
                    product_data['attributes'] = attribute_variants
        return response_success(product_data)

    def get_product_id_import(self, convert, product, products_ext):
        return product['ID']

    def check_product_import(self, convert, product, products_ext):
        product_id = self.get_map_field_by_src(
            self.TYPE_PRODUCT,
            convert['id'],
            convert['code'],
            lang=self._notice['target']['language_default']
        )

        # break if not exists
        if not product_id:
            return False

        # insert all attributes to woocommerce_attribute_taxonomies
        if convert['attributes']:
            for attr in convert['attributes']:
                # check exists id_src type attr on migration_map
                migration_map = self.select_map(self._migration_id, self.TYPE_ATTR, attr['attribute_id'])

                if not migration_map:
                    woocommerce_attribute_taxonomies = {
                        'attribute_name': attr['attribute_code'],
                        'attribute_label': attr['attribute_name'],
                        'attribute_type': attr['attribute_type'],
                        'attribute_orderby': 'menu_order',
                        'attribute_public': 1
                    }
                    woocommerce_attribute_taxonomies_query = self.create_insert_query_connector(
                        'woocommerce_attribute_taxonomies',
                        woocommerce_attribute_taxonomies
                    )
                    woocommerce_attribute_taxonomies_import_id = self.import_data_connector(
                        woocommerce_attribute_taxonomies_query,
                        self.TYPE_ATTR,
                        product_id
                    )
                    if not woocommerce_attribute_taxonomies_import_id:
                        self.log(
                            'woocommerce_attribute_taxonomies[' + to_str(woocommerce_attribute_taxonomies) + '] cannot insert !',
                            'woocommerce_attribute_taxonomies_log'
                        )
                        return

                    self.insert_map(
                        self.TYPE_ATTR, attr['attribute_id'],
                        woocommerce_attribute_taxonomies_import_id,
                        attr['attribute_code']
                    )

        # insert attribute for side by product
        product_attribute = dict()
        position = 1
        for pa in convert['product_attribute']:
            attr_map_code_src = self.get_map_field_by_src(self.TYPE_ATTR, pa['attribute_id'], field='code_src')
            attr_map_value = self.select_map(
                self._migration_id,
                self.TYPE_ATTR_VALUE,
                code_src=pa['attribute_id'],
                value=pa['value']
            )

            global term_taxonomy_attr_import_id
            if not attr_map_value:
                # insert attr to term
                term_attr = {
                    'name': pa['value'],
                    'slug': self.sanitize_title(pa['value']),
                    'term_group': 0,
                }
                term_attr_query = self.create_insert_query_connector('terms', term_attr)
                term_attr_import_id = self.import_data_connector(term_attr_query, self.TYPE_ATTR, product_id)
                if not term_attr_import_id:
                    self.log(
                        'type: attr terms[' + to_str(term_attr) + '] cannot insert !',
                        'term_attr_log'
                    )
                    return

                # insert data attr to termmeta
                termmeta_attr = {
                    'term_id': term_attr_import_id,
                    'meta_key': 'order_pa_' + attr_map_code_src,
                    'meta_value': 0
                }
                termmeta_attr_query = self.create_insert_query_connector('termmeta', termmeta_attr)
                termmeta_attr_import_id = self.import_data_connector(termmeta_attr_query, self.TYPE_ATTR,
                                                                     term_attr_import_id)
                if not termmeta_attr_import_id:
                    self.log(
                        'type: attr termmeta[' + to_str(termmeta_attr) + '] cannot insert !',
                        'termmeta_log'
                    )
                    return

                # insert attr to term_taxonomy
                term_taxonomy_attr = {
                    'term_id': term_attr_import_id,
                    'taxonomy': 'pa_' + attr_map_code_src,
                    'description': pa['value'],
                    'parent': 0,
                    'count': 0
                }
                term_taxonomy_attr_query = self.create_insert_query_connector('term_taxonomy', term_taxonomy_attr)
                term_taxonomy_attr_import_id = self.import_data_connector(term_taxonomy_attr_query, self.TYPE_ATTR,
                                                                          term_attr_import_id)

                self.insert_map(
                    self.TYPE_ATTR_VALUE,
                    id_src=pa['value_id'],
                    id_desc=term_attr_import_id,
                    code_src=pa['attribute_id'],
                    code_desc=term_taxonomy_attr_import_id,
                    value=pa['value']
                )
            else:
                term_taxonomy_attr_import_id = attr_map_value['code_desc']

            # term_relationships
            term_relationship_attr = {
                'object_id': product_id,
                'term_taxonomy_id': term_taxonomy_attr_import_id,
                'term_order': 0
            }
            # drop if exists term_relationships
            del_term_relationships_query = {
                'type': 'query',
                'query': "DELETE FROM _DBPRF_term_relationships "
                         + " WHERE object_id = " + to_str(product_id)
                         + " AND term_taxonomy_id = " + to_str(term_taxonomy_attr_import_id),
            }
            self.get_connector_data(
                self.get_connector_url('query'),
                {'query': json.dumps(del_term_relationships_query)}
            )
            # re-import term_relationships
            term_relationship_query = self.create_insert_query_connector(
                'term_relationships',
                term_relationship_attr
            )
            term_relationship_import_id = self.import_data_connector(
                term_relationship_query,
                self.TYPE_ATTR,
                product_id
            )
            if not term_relationship_import_id:
                self.log(
                    'term_relationship[' + to_str(term_relationship_attr) + '] cannot insert !',
                    'term_relationship_log'
                )
                return

            # create meta_value for _product_attributes on postmeta
            product_attribute['pa_' + attr_map_code_src] = {
                'name': 'pa_' + attr_map_code_src,
                'value': '',
                'position': position,
                'is_visible': 1,
                'is_variation': 0,
                'is_taxonomy': 1
            }
            position += 1

        # update value _product_attributes to postmeta
        data_update = {
            'meta_value': php_serialize(product_attribute)
        }
        condition = {
            'post_id': product_id,
            'meta_key': '_product_attributes'
        }
        postmeta_update_query = self.create_update_query_connector('postmeta', data_update, condition)
        postmeta_update_id = self.import_data_connector(postmeta_update_query, self.TYPE_PRODUCT, product_id)
        if not postmeta_update_id:
            self.log(
                'query: ' + to_str(postmeta_update_query) + ' cannot execute !',
                'postmeta_update_query_log'
            )
            return

        # update image for postmeta
        global _thumbnail_id
        global src_path

        path_default = ['BSNIDESS1.39VANNPW-100.png', 'chpultmtchoc60ct-s.png']

        url = convert['thumb_image']['url']
        path = convert['thumb_image']['path']
        if url and path:
            if path in path_default:
                image_import_path = self.uploadImageConnector(
                    self.process_image_before_import(url, path),
                    self.add_prefix_path(
                        self.make_woocommerce_image_path(path, self.TYPE_PRODUCT),
                        'wp-content/uploads'.rstrip('/')
                    )
                )
                if image_import_path:
                    src_path = self.remove_prefix_path(image_import_path, 'wp-content/uploads'.rstrip('/'))
                    image_details = self.get_sizes(url)
                    _thumbnail_id = self.wp_image(src_path, image_details)
            else:
                _thumbnail_id = None

        if _thumbnail_id:
            data_img_update = {
                'meta_value': _thumbnail_id
            }
            condition_img = {
                'post_id': product_id,
                'meta_key': '_thumbnail_id'
            }
            postmeta_update_img_query = self.create_update_query_connector('postmeta', data_img_update, condition_img)
            product_update_img_id = self.import_data_connector(postmeta_update_img_query, self.TYPE_PRODUCT, product_id)
            if not product_update_img_id:
                self.log(
                    'query: ' + to_str(postmeta_update_img_query) + ' cannot execute !',
                    'update_img_postmeta_log'
                )
                return

        return True

    def update_latest_data_product(self, product_id, convert, product, products_ext):
        all_query = list()
        language_code = convert.get('language_code')
        if self.is_wpml() and not language_code:
            language_code = self._notice['target']['language_default']
        # todo: update product name
        # begin
        product_query = self.create_update_query_connector("posts", {'ID': product_id, 'post_title': convert['name']},
                                                           {'ID': product_id})
        all_query.append(product_query)

        # end

        old_url_key = self.get_map_field_by_src(self.TYPE_PRODUCT, convert['id'], convert['code'], 'code_desc')

        # todo: update product category
        # begin
        category_desc = self.select_all_category_map()
        all_categories = list()
        for category in convert['categories']:
            category_id = self.get_map_field_by_src(self.TYPE_CATEGORY, category['id'], category['code'],
                                                    lang=language_code)
            if not category_id:
                category_id = self.get_map_field_by_src(self.TYPE_CATEGORY, None, category['code'], lang=language_code)
            if not category_id:
                category_id = self.get_map_field_by_src(self.TYPE_CATEGORY, category['id'], None, lang=language_code)
            if category_id:
                all_categories.append(category_id)
        all_categories = list(set(all_categories))
        # todo: delete old category product
        query_cate = {
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_term_relationships` WHERE `object_id` = " + to_str(
                product_id) + " AND `term_taxonomy_id` IN " + self.list_to_in_condition(category_desc) + ""
        }
        self.query_data_connector(query_cate, 'update_product')
        for cate_id in all_categories:
            query_cate_prod = {
                'type': 'select',
                'query': "SELECT * FROM `_DBPRF_term_relationships` WHERE `object_id` = " + to_str(
                    product_id) + " AND `term_taxonomy_id` = " + to_str(cate_id) + ""
            }
            check_product_category = self.select_data_connector(query_cate_prod, 'category_product')
            if (not check_product_category) or check_product_category['result'] != 'success' or (
                    to_len(check_product_category['data']) == 0):
                category_data = {
                    'object_id': product_id,
                    'term_taxonomy_id': cate_id,
                    'term_order': 0
                }
                category_query = self.create_insert_query_connector("term_relationships", category_data)
                all_query.append(category_query)

        # End
        stock_status = 'instock'
        if 'is_in_stock' in convert:
            stock_status = 'instock' if convert['is_in_stock'] else 'outofstock'
        else:
            stock_status = 'outofstock' if convert['manage_stock'] else 'instock'
        tax_class = ''
        if convert['tax']['id'] or convert['tax']['code']:
            tax_class = self.get_map_field_by_src(self.TYPE_TAX, convert['tax']['id'], convert['tax']['code'],
                                                  field='code_desc')
        product_meta = {
            '_stock_status': stock_status,
            '_downloadable': "yes" if convert['type'] == self.PRODUCT_DOWNLOAD else "no",
            '_virtual': "yes" if convert['type'] == self.PRODUCT_VIRTUAL else "no",
            '_regular_price': convert['price'],
            '_sale_price': convert['special_price']['price'] if convert['special_price']['price'] and (
                    self.to_timestamp(convert['special_price']['end_date']) > time.time() or (
                    convert['special_price']['end_date'] == '0000-00-00' or convert['special_price'][
                'end_date'] == '0000-00-00 00:00:00') or convert['special_price']['end_date'] == '' or
                    convert['special_price']['end_date'] == None) else "",
            '_tax_status': convert['tax'].get('status', (
                "taxable" if to_int(convert['tax']['id']) or convert['tax']['code'] else "none")),
            '_tax_class': tax_class if tax_class else '',
            '_weight': convert['weight'] if convert['weight'] else '',
            '_length': convert['length'] if convert['length'] else '',
            '_width': convert['width'] if convert['width'] else '',
            '_height': convert['height'] if convert['height'] else '',
            '_sku': convert['sku'],
            '_price': convert['special_price']['price'] if convert['special_price']['price'] and (
                    self.to_timestamp(convert['special_price']['end_date']) > time.time() or (
                    convert['special_price']['end_date'] == '0000-00-00' or convert['special_price'][
                'end_date'] == '0000-00-00 00:00:00' or convert['special_price']['end_date'] == '' or
                    convert['special_price']['end_date'] == None)) else convert['price'],
            '_manage_stock': "yes" if convert['manage_stock'] or convert['manage_stock'] == True else "no",
            '_stock': convert['qty'] if convert['qty'] else 0,
            # 'show_on_pos': '1' if convert['pos'] else 0,
        }
        if convert['special_price']['start_date'] and (
                self.to_timestamp(convert['special_price']['end_date']) > time.time() or (
                convert['special_price']['end_date'] == '0000-00-00' or convert['special_price'][
            'end_date'] == '0000-00-00 00:00:00' or convert['special_price']['end_date'] == '' or
                convert['special_price']['end_date'] == None)):
            product_meta['_sale_price_dates_from'] = self.to_timestamp(convert['special_price']['start_date'])
        if convert['special_price']['end_date'] and (
                self.to_timestamp(convert['special_price']['end_date']) > time.time() or (
                convert['special_price']['end_date'] == '0000-00-00' or convert['special_price'][
            'end_date'] == '0000-00-00 00:00:00' or convert['special_price']['end_date'] == '' or
                convert['special_price']['end_date'] == None)):
            product_meta['_sale_price_dates_to'] = self.to_timestamp(convert['special_price']['end_date'])
        if 'group_prices' in convert and to_len(convert['group_prices']) > 0:
            product_meta['wholesale_customer_wholesale_price'] = convert['group_prices'][0]['price']
        all_meta_queries = list()
        for meta_key, meta_value in product_meta.items():
            meta_insert = {
                'post_id': product_id,
                'meta_key': meta_key,
                'meta_value': meta_value
            }
            if meta_key == '_sale_price_dates_from' or meta_key == '_sale_price_dates_to':
                query_meta_key = {
                    'type': 'select',
                    'query': "SELECT * FROM `_DBPRF_postmeta` WHERE `post_id` = " + to_str(
                        product_id) + " AND `meta_key` = " + to_str(meta_key) + ""
                }
                check_meta_key = self.select_data_connector(query_meta_key, 'postmeta')
                if (not check_meta_key) or check_meta_key['result'] != 'success' or (not check_meta_key['data']) or (
                        to_len(check_meta_key['data']) == 0):
                    sale_price_data = {
                        'post_id': product_id,
                        'meta_key': meta_key,
                        'meta_value': meta_value
                    }
                    meta_price_query = self.create_insert_query_connector("postmeta", sale_price_data)
                    all_query.append(meta_price_query)
            meta_query = self.create_update_query_connector("postmeta", meta_insert,
                                                            {'post_id': product_id, 'meta_key': meta_key})
            all_query.append(meta_query)
        # todo: update children
        children_list = list()
        option_list = list()
        if convert['children']:
            children_list = convert['children']
        else:
            if convert['options']:
                option_list = convert['options']
                if self.count_child_from_option(convert['options']) <= self.VARIANT_LIMIT:
                    children_list = self.convert_option_to_child(option_list, convert)

        if children_list and to_len(children_list) <= self.VARIANT_LIMIT:
            for key_child, product_child in enumerate(children_list):
                children_id = self.get_map_field_by_src(self.TYPE_CHILD, product_child['id'], product_child['code'],
                                                        lang=language_code)
                if not children_id:
                    continue

                if product_child.get('is_in_stock'):
                    child_stock_status = 'instock' if product_child['is_in_stock'] else 'outofstock'
                else:
                    child_stock_status = 'outofstock' if product_child['manage_stock'] else 'instock'
                children_meta = {
                    '_stock_status': child_stock_status,
                    '_sku': product_child['sku'] if product_child['sku'] else '',
                    '_weight': product_child['weight'] if product_child['weight'] else '',
                    '_length': product_child['length'] if product_child['length'] else '',
                    '_width': product_child['width'] if product_child['width'] else '',
                    '_height': product_child['height'] if product_child['height'] else '',
                    '_manage_stock': "yes" if product_child['manage_stock'] else "no",
                    '_stock': product_child['qty'] if product_child['qty'] else 0,
                    '_regular_price': product_child['price'],
                    '_sale_price': product_child['special_price']['price'] if product_child['special_price'][
                                                                                  'price'] and (self.to_timestamp(
                        product_child['special_price']['end_date']) > time.time() or (product_child['special_price'][
                                                                                          'end_date'] == '0000-00-00' or
                                                                                      product_child['special_price'][
                                                                                          'end_date'] == '0000-00-00 00:00:00' or
                                                                                      convert['special_price'][
                                                                                          'end_date'] == '' or
                                                                                      convert['special_price'][
                                                                                          'end_date'] == None)) else
                    product_child['price'],
                    '_price': product_child['special_price']['price'] if product_child['special_price']['price'] and (
                            self.to_timestamp(product_child['special_price']['end_date']) > time.time() or (
                            product_child['special_price']['end_date'] == '0000-00-00' or
                            product_child['special_price']['end_date'] == '0000-00-00 00:00:00' or
                            convert['special_price']['end_date'] == '' or convert['special_price'][
                                'end_date'] == None)) else product_child['price'],
                }
                if product_child['special_price']['price'] and (
                        self.to_timestamp(product_child['special_price']['end_date']) > time.time() or (
                        product_child['special_price']['end_date'] == '0000-00-00' or product_child['special_price'][
                    'end_date'] == '0000-00-00 00:00:00' or convert['special_price']['end_date'] == '' or
                        convert['special_price']['end_date'] == None)):
                    if product_child['special_price']['start_date']:
                        children_meta['_sale_price_dates_from'] = self.to_timestamp(
                            product_child['special_price']['start_date'])
                    if product_child['special_price']['end_date']:
                        children_meta['_sale_price_dates_to'] = self.to_timestamp(
                            product_child['special_price']['end_date'])

                for meta_key, meta_value in children_meta.items():
                    meta_insert_child = {
                        'post_id': children_id,
                        'meta_key': meta_key,
                        'meta_value': meta_value
                    }

                    if meta_key == '_sale_price_dates_from' or meta_key == '_sale_price_dates_to':
                        query_meta_key = {
                            'type': 'select',
                            'query': "SELECT * FROM `_DBPRF_postmeta` WHERE `post_id` = " + to_str(
                                children_id) + " AND `meta_key` = " + to_str(meta_key) + ""
                        }
                        check_meta_key = self.select_data_connector(query_meta_key, 'postmeta')
                        if (not check_meta_key) or check_meta_key['result'] != 'success' or (
                                not check_meta_key['data']) or (to_len(check_meta_key['data']) == 0):
                            sale_price_data = {
                                'post_id': children_id,
                                'meta_key': meta_key,
                                'meta_value': meta_value
                            }
                            meta_price_query = self.create_insert_query_connector("postmeta", sale_price_data)
                            all_query.append(meta_price_query)

                    meta_query_child = self.create_update_query_connector('postmeta', meta_insert_child,
                                                                          {'post_id': children_id,
                                                                           'meta_key': meta_key})
                    all_query.append(meta_query_child)
        # todo: seo
        # begin
        if self.is_exist_lecm_rewrite():
            if (self._notice['config']['seo'] or self._notice['config']['seo_301']) and convert['seo']:
                delete_query = list()
                delete_query.append(
                    self.create_delete_query_connector('lecm_rewrite', {'type': 'product', 'type_id': product_id}))
                self.query_multiple_data_connector(delete_query)
                for seo_url in convert['seo']:
                    if not seo_url['request_path']:
                        continue
                    if old_url_key != seo_url['request_path'].replace(' ', ''):
                        query_check = {
                            'link': seo_url['request_path']
                        }
                        if self.is_wpml() and convert.get('language_code'):
                            query_check['lang'] = convert['language_code']
                        seo_query = {
                            'type': 'select',
                            'query': "SELECT * FROM _DBPRF_lecm_rewrite WHERE " + self.dict_to_where_condition(
                                query_check)
                        }
                        check_seo_exit = self.select_data_connector(seo_query, 'lecm_rewrite')
                        if check_seo_exit and check_seo_exit['result'] == 'success' and to_len(
                                check_seo_exit['data']) > 0:
                            continue
                        else:
                            le_url_rewrite = {
                                'link': to_str(seo_url['request_path']).rstrip('/'),
                                'type': 'product',
                                'type_id': product_id
                            }
                            if self.is_wpml():
                                le_url_rewrite['lang'] = convert.get('language_code')
                            if self._notice['config']['seo_301']:
                                le_url_rewrite['redirect_type'] = 301
                            self.import_data_connector(
                                self.create_insert_query_connector("lecm_rewrite", le_url_rewrite), 'seo_product')

        self.import_multiple_data_connector(all_query, 'update_product')
        if self.is_wpml() and not convert.get('language_code'):
            where_product_wpml = {
                'migration_id': self._migration_id,
                'type': 'product',
            }
            if convert['id']:
                where_product_wpml['id_src'] = convert['id']
            else:
                where_product_wpml['code'] = convert['code']
            product_wpml = self.select_obj(TABLE_MAP, where_product_wpml)
            if product_wpml['result'] == 'success' and product_wpml['data']:
                for product_wpml_row in product_wpml['data']:
                    if product_wpml_row['id_desc'] == product_id or not product_wpml_row.get('lang'):
                        continue
                    convert_wpml = self.get_convert_data_language(convert, target_language_id=language_code)
                    convert_wpml['language_code'] = product_wpml_row['lang']
                    self.update_latest_data_product(product_wpml_row['id_desc'], convert_wpml, product, products_ext)
        return response_success()

    def update_product_after_demo(self, product_id, convert, product, products_ext):
        language_code = convert.get('language_code')
        if self.is_wpml() and not language_code:
            language_code = self._notice['target']['language_default']
        all_queries = list()
        query_delete = {
            'type': 'delete',
            'query': 'DELETE FROM _DBPRF_term_relationships WHERE object_id = ' + to_str(
                product_id) + ' AND term_taxonomy_id IN (SELECT term_taxonomy_id FROM _DBPRF_term_taxonomy WHERE taxonomy IN ' + self.list_to_in_condition(
                ['product_brand', 'product_cat']) + ')'
        }
        all_queries.append(query_delete)
        # category
        all_categories = list()
        if convert['categories']:
            for category in convert['categories']:
                category_id = self.get_map_field_by_src(self.TYPE_CATEGORY, category['id'], category['code'],
                                                        language_code)
                if not category_id:
                    category_id = self.get_map_field_by_src(self.TYPE_CATEGORY, None, category['code'], language_code)
                if not category_id:
                    category_id = self.get_map_field_by_src(self.TYPE_CATEGORY, category['id'], None, language_code)
                if category_id:
                    all_categories.append(category_id)
            all_categories = list(set(all_categories))
            for cate_id in all_categories:
                category_data = {
                    'object_id': product_id,
                    'term_taxonomy_id': cate_id,
                    'term_order': 0
                }
                category_query = self.create_insert_query_connector("term_relationships", category_data)
                all_queries.append(category_query)
        if self._notice['target']['support']['manufacturers']:
            if convert['manufacturer']['id'] or convert['manufacturer']['name']:
                manufacturer_id = self.get_map_field_by_src(self.TYPE_MANUFACTURER, convert['manufacturer']['id'])
                if not manufacturer_id:
                    manufacturer_id = self.get_map_field_by_src(self.TYPE_MANUFACTURER, None,
                                                                convert['manufacturer']['id'])
                if manufacturer_id:
                    relationship_data = {
                        'object_id': product_id,
                        'term_taxonomy_id': manufacturer_id,
                        'term_order': 0
                    }
                    category_query = self.create_insert_query_connector("term_relationships", relationship_data)
                    all_queries.append(category_query)
                elif convert['manufacturer']['name']:
                    slug = self.sanitize_title(convert['manufacturer']['name'])
                    manufacturer_term = {
                        'name': convert['manufacturer']['name'],
                        'slug': slug,
                        'term_group': 0,
                    }
                    manufacturer_term_query = self.create_insert_query_connector('terms', manufacturer_term)
                    term_id = self.import_data_connector(manufacturer_term_query, 'manufacturer')
                    if not term_id:
                        return response_warning('Manufacturer ' + to_str(convert['id']) + ' import false.')
                    manufacturer_taxonomy = {
                        'term_id': term_id,
                        'taxonomy': 'product_brand',
                        'description': '',
                        'parent': 0,
                        'count': 0
                    }
                    manufacturer_taxonomy_query = self.create_insert_query_connector('term_taxonomy',
                                                                                     manufacturer_taxonomy)
                    manufacturer_taxonomy_import = self.import_manufacturer_data_connector(manufacturer_taxonomy_query,
                                                                                           True, convert['id'])

                    if manufacturer_taxonomy_import:
                        relationship_data = {
                            'object_id': product_id,
                            'term_taxonomy_id': manufacturer_id,
                            'term_order': 0
                        }
                        relationship_query = self.create_insert_query_connector("term_relationships", relationship_data)
                        all_queries.append(relationship_query)
                        self.insert_map(self.TYPE_MANUFACTURER, convert['manufacturer']['id'],
                                        manufacturer_taxonomy_import, convert['manufacturer']['name'])

        if convert['tax']['id'] or convert['tax']['code']:
            tax_class = self.get_map_field_by_src(self.TYPE_TAX, convert['tax']['id'], convert['tax']['code'],
                                                  'code_desc')
            if tax_class:
                meta_insert = {
                    'post_id': product_id,
                    'meta_key': '_tax_class',
                    'meta_value': tax_class
                }
                where_meta = {
                    'post_id': product_id,
                    'meta_key': '_tax_class',
                }
                all_queries.append(self.create_update_query_connector('postmeta', meta_insert, where_meta))
        self.import_multiple_data_connector(all_queries, 'update_product')
        return response_success()

    def router_product_import(self, convert, product, products_ext):
        return response_success('product_import')

    def before_product_import(self, convert, product, products_ext):
        return response_success()

    def product_import(self, convert, product, products_ext):
        # resource data: product
        self.log(convert, 'convert_product')

        # insert to posts [type: product]
        posts_product = {
            'post_author': 1,
            'post_date': convert['created_at'] if convert['created_at'] else get_current_time(),
            'post_date_gmt': convert['created_at'] if convert['created_at'] else get_current_time(),
            'post_content': convert['description'],
            'post_title': convert['name'],
            'post_excerpt': convert['short_description'],
            'post_status': 'publish',
            'comment_status': 'open',
            'ping_status': 'closed',
            'post_password': '',
            'post_name': convert['name'],
            'to_ping': '',
            'pinged': '',
            'post_modified': convert['updated_at'] if convert['updated_at'] else get_current_time(),
            'post_modified_gmt': convert['updated_at'] if convert['updated_at'] else get_current_time(),
            'post_content_filtered': '',
            'post_parent': 0,
            'guid': '',
            'menu_order': 0,
            'post_type': self.TYPE_PRODUCT,
            'post_mime_type': '',
            'comment_count': 0
        }

        posts_query = self.create_insert_query_connector('posts', posts_product)
        post_import_id = self.import_data_connector(posts_query, self.TYPE_PRODUCT, convert['id'])
        if not post_import_id:
            return response_warning('posts[' + to_str(posts_product) + '] cannot insert !')
        posts_update = {
            'guid': 'http://localhost/wordpress/?post_type=product&#038;p=' + to_str(post_import_id)
        }
        update_query = self.create_update_query_connector('posts', posts_update, {'ID': post_import_id})
        update_post_id = self.import_data_connector(update_query, self.TYPE_PRODUCT, convert['id'])
        if not update_post_id:
            return response_warning('query: ' + to_str(update_query) + ' cannot execute !')
        self.insert_map(self.TYPE_PRODUCT, convert['id'], post_import_id)
        return response_success(post_import_id)

    def after_product_import(self, product_id, convert, product, products_ext):
        # get id image on posts
        global image_id
        image_path = convert['thumb_image']['url'] + convert['thumb_image']['path']
        if image_path:
            search_image_query = {
                'type': 'select',
                'query': "SELECT p.ID FROM _DBPRF_posts p WHERE p.guid LIKE '" + image_path + "' LIMIT 1"
            }
            image_search = self.get_connector_data(
                self.get_connector_url('query'),
                {'query': json.dumps(search_image_query)}
            )
            image_id = image_search['data'][0]['ID'] if len(image_search['data']) > 0 else None
        # insert to postmeta
        meta_key = {
            '_edit_lock': to_str(int(time.time())) + ':1',
            '_edit_last': '1',
            '_sku': convert['sku'],
            '_regular_price': convert['products_msrp'],
            '_sale_price': convert['products_wholesale_price'],
            'total_sales': 0,
            '_tax_status': 'taxable',
            '_tax_class': '',
            '_manage_stock': convert['manage_stock'],
            '_backorders': 'no',
            '_sold_individually': 'no',
            '_weight': convert['weight'],
            '_length': convert['length'],
            '_width': convert['width'],
            '_height': convert['height'],
            '_virtual': '',
            '_downloadable': 'no',
            '_download_limit': 'no',
            '_download_expiry': '-1',
            '_stock': convert['qty'],
            '_stock_status': 'instock',
            '_wc_average_rating': 0,
            '_wc_review_count': convert['products_viewed'],
            '_product_version': '4.9.0',
            '_price': convert['products_wholesale_price'],
            '_thumbnail_id': image_id,
            '_product_image_gallery': image_id,
            '_product_attributes': '',
        }

        for key, value in meta_key.items():
            postmeta = {
                'post_id': product_id,
                'meta_key': key,
                'meta_value': value
            }
            postmeta_query = self.create_insert_query_connector('postmeta', postmeta)
            postmeta_import = self.import_data_connector(postmeta_query, self.TYPE_PRODUCT, product_id)
            if not postmeta_import:
                return response_warning('postmeta[' + to_str(postmeta) + '] cannot insert !')

        # insert to wc_product_meta_lookup
        default_price = convert['products_wholesale_price'] if convert['products_wholesale_price'] else convert[
            'products_msrp']
        wc_product_meta_lookup = {
            'product_id': product_id,
            'sku': convert['sku'],
            'virtual': 0,
            'downloadable': 0,
            'min_price': default_price,
            'max_price': default_price,
            'onsale': 1 if convert['products_wholesale_price'] else 0,
            'stock_quantity': convert['qty'],
            'stock_status': 'instock' if int(convert['qty']) > 0 else 'outofstock',
            'rating_count': 0,
            'average_rating': 0.0,
            'total_sales': 0,
            'tax_status': 'taxable',
            'tax_class': ''
        }

        wc_product_meta_lookup_query = self.create_insert_query_connector('wc_product_meta_lookup',
                                                                          wc_product_meta_lookup)
        wc_product_meta_lookup_import_id = self.import_data_connector(wc_product_meta_lookup_query, self.TYPE_PRODUCT,
                                                                      product_id)
        if not wc_product_meta_lookup_import_id:
            return response_warning('wc_product_meta_lookup[' + to_str(wc_product_meta_lookup) + '] cannot insert !')

        # insert to term_relationships
        term_relationships = {
            'object_id': '',
            'term_taxonomy_id': '',
            'term_order': 0,
        }

        PRODUCT_SIMPLE_ID = 2  # default value is simple (term_id = 2)
        c_id = PRODUCT_SIMPLE_ID
        for c in convert['categories']:
            c_map_id = self.get_map_field_by_src(self.TYPE_CATEGORY, c['id'])
            if c_map_id:
                c_id = c_map_id

                term_relationships['object_id'] = product_id
                term_relationships['term_taxonomy_id'] = c_id

                term_relationships_query = self.create_insert_query_connector('term_relationships', term_relationships)
                term_relationships_import_id = self.import_data_connector(term_relationships_query, self.TYPE_PRODUCT,
                                                                          product_id)
                if not term_relationships_import_id:
                    return response_warning('term_relationships[' + to_str(term_relationships) + '] cannot insert !')

        if c_id == PRODUCT_SIMPLE_ID:
            term_relationships['object_id'] = product_id
            term_relationships['term_taxonomy_id'] = c_id

            term_relationships_query = self.create_insert_query_connector('term_relationships', term_relationships)
            term_relationships_import_id = self.import_data_connector(term_relationships_query, self.TYPE_PRODUCT,
                                                                      product_id)
            if not term_relationships_import_id:
                return response_warning('term_relationships[' + to_str(term_relationships) + '] cannot insert !')
        return response_success(None, 'Migration data from product to post complete !')

    def addition_product_import(self, convert, product, products_ext):
        return response_success()

    def finish_product_import(self):
        if self.is_variant_limit:
            self._notice['config']['variant_limit'] = True
        return response_success()

    # TODO: CUSTOMER
    def prepare_customers_import(self):
        if self._notice['config'].get('cus_pass'):
            delete_query = {
                'type': 'query',
                'query': "DELETE FROM `_DBPRF_options` WHERE option_name = 'LEPP_TYPE' OR option_name = 'LEPP_URL'"
            }
            config_delete = self.import_data_connector(delete_query)
            all_queries = list()
            type_data = {
                'option_name': 'LEPP_TYPE',
                'option_value': self._notice['src']['cart_type'],
                'autoload': 'yes'
            }
            type_query = self.create_insert_query_connector('options', type_data)
            all_queries.append(type_query)
            url_data = {
                'option_name': 'LEPP_URL',
                'option_value': self._notice['src']['cart_url'],
                'autoload': 'yes'
            }
            url_query = self.create_insert_query_connector('options', url_data)
            all_queries.append(url_query)

            if all_queries:
                self.import_multiple_data_connector(all_queries, 'customer')
        return self

    def get_customers_main_export(self):
        id_src = self._notice['process']['customers']['id_src']
        limit = self._notice['setting']['customers']
        prefix = self._notice['src']['config']['table_prefix']
        if self._notice['src']['config'].get('site_id'):
            prefix = to_str(prefix).replace(to_str(self._notice['src']['config'].get('site_id')) + '_', '')
        query = {
            'type': 'select',
            'query': "SELECT * FROM " + prefix + "users u LEFT JOIN " + prefix + "usermeta um ON u.ID = um.user_id WHERE (um.meta_key = '_DBPRF_capabilities' AND um.meta_value LIKE '%customer%' OR um.meta_value LIKE '%subscriber%') AND ID > " + to_str(
                id_src) + " ORDER BY ID ASC LIMIT " + to_str(limit)
        }
        # customers = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        customers = self.select_data_connector(query, 'customers')
        if not customers or customers['result'] != 'success':
            return response_error()
        return customers

    def get_customers_ext_export(self, customers):
        url_query = self.get_connector_url('query')
        customers_ids = duplicate_field_value_from_list(customers['data'], 'ID')
        customer_ext_queries = {
            'user_meta': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_usermeta WHERE user_id IN " + self.list_to_in_condition(
                    customers_ids),
            }
        }

        if self._notice['src']['support'].get('customer_point_rewards'):
            customer_ext_queries['wc_points_rewards_user_points'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_wc_points_rewards_user_points WHERE (order_id IS NULL OR order_id = '') AND user_id IN " + self.list_to_in_condition(
                    customers_ids),
            }
            customer_ext_queries['wc_points_rewards_user_points_log'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_wc_points_rewards_user_points_log WHERE (order_id IS NULL OR order_id = '') AND user_id IN " + self.list_to_in_condition(
                    customers_ids),
            }

        # customers_ext = self.get_connector_data(url_query,
        #                                         {'serialize': True, 'query': json.dumps(customer_ext_queries)})
        customers_ext = self.select_multiple_data_connector(customer_ext_queries, 'customers')
        if not customers_ext or customers_ext['result'] != 'success':
            return response_error()

        return customers_ext

    def convert_customer_export(self, customer, customers_ext):
        customer_data = self.construct_customer()
        customer_data = self.add_construct_default(customer_data)
        customer_data['id'] = customer['ID']
        customer_data['code'] = customer['user_login']
        customer_data['username'] = customer['user_nicename']
        customer_data['email'] = customer['user_email']
        customer_data['password'] = customer['user_pass']
        customer_data['website'] = customer['user_url']
        customer_data['user_url'] = customer['user_url']
        customer_data['active'] = True
        customer_data['created_at'] = convert_format_time(customer['user_registered'])
        customer_meta = get_list_from_list_by_field(customers_ext['data']['user_meta'], 'user_id', customer['ID'])
        customer_data['first_name'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'first_name',
                                                                       'meta_value')
        customer_data['last_name'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'last_name',
                                                                      'meta_value')

        prefix = self._notice['src']['config']['table_prefix']
        capabilities = to_str(prefix) + '_capabilities'
        customer_data['capabilities'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', capabilities,
                                                                         'meta_value')
        # billing
        address_data = self.construct_customer_address()
        address_data['code'] = to_str(customer['ID']) + "_1"
        address_data['first_name'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'billing_first_name',
                                                                      'meta_value')
        address_data['last_name'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'billing_last_name',
                                                                     'meta_value')
        address_data['address_1'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'billing_address_1',
                                                                     'meta_value')
        address_data['address_2'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'billing_address_2',
                                                                     'meta_value')
        address_data['city'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'billing_city', 'meta_value')
        address_data['postcode'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'billing_postcode',
                                                                    'meta_value')
        address_data['telephone'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'billing_phone',
                                                                     'meta_value')
        address_data['company'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'billing_company',
                                                                   'meta_value')
        address_data['fax'] = get_row_value_from_list_by_field(customer_meta, 'meta_key', 'billing_fax', 'meta_value')
        address_data['country']['country_code'] = get_row_value_from_list_by_field(customer_meta, 'meta_key',
                                                                                   'billing_country', 'meta_value')
        address_data['country']['code'] = address_data['country']['country_code']
        address_data['country']['name'] = self.get_country_name_by_code(address_data['country']['country_code'])
        address_data['state']['state_code'] = get_row_value_from_list_by_field(customer_meta, 'meta_key',
                                                                               'billing_state', 'meta_value')
        address_data['state']['code'] = address_data['state']['state_code']
        address_data['default']['billing'] = True
        if address_data['address_1'] or address_data['address_2']:
            customer_data['address'].append(address_data)
        # shipping
        shipping_address = self.get_list_from_list_by_field_as_first_key(customer_meta, 'meta_key', 'shipping_')
        if shipping_address:
            shipping_data = self.construct_customer_address()
            shipping_data['code'] = to_str(customer['ID']) + "_2"
            shipping_data['first_name'] = get_row_value_from_list_by_field(shipping_address, 'meta_key',
                                                                           'shipping_first_name', 'meta_value')
            shipping_data['last_name'] = get_row_value_from_list_by_field(shipping_address, 'meta_key',
                                                                          'shipping_last_name', 'meta_value')
            shipping_data['address_1'] = get_row_value_from_list_by_field(shipping_address, 'meta_key',
                                                                          'shipping_address_1', 'meta_value')
            shipping_data['address_2'] = get_row_value_from_list_by_field(shipping_address, 'meta_key',
                                                                          'shipping_address_2', 'meta_value')
            shipping_data['city'] = get_row_value_from_list_by_field(shipping_address, 'meta_key', 'shipping_city',
                                                                     'meta_value')
            shipping_data['postcode'] = get_row_value_from_list_by_field(shipping_address, 'meta_key',
                                                                         'shipping_postcode', 'meta_value')
            shipping_data['telephone'] = get_row_value_from_list_by_field(shipping_address, 'meta_key',
                                                                          'shipping_phone', 'meta_value')
            shipping_data['company'] = get_row_value_from_list_by_field(shipping_address, 'meta_key',
                                                                        'shipping_company', 'meta_value')
            shipping_data['fax'] = get_row_value_from_list_by_field(shipping_address, 'meta_key', 'shipping_fax',
                                                                    'meta_value')
            shipping_data['country']['country_code'] = get_row_value_from_list_by_field(shipping_address, 'meta_key',
                                                                                        'shipping_country',
                                                                                        'meta_value')
            shipping_data['country']['code'] = shipping_data['country']['country_code']
            shipping_data['country']['name'] = self.get_country_name_by_code(shipping_data['country']['code'])
            shipping_data['state']['state_code'] = get_row_value_from_list_by_field(shipping_address, 'meta_key',
                                                                                    'shipping_state', 'meta_value')
            shipping_data['state']['code'] = shipping_data['state']['state_code']
            shipping_data['default']['shipping'] = True
            if shipping_data['address_1'] or shipping_data['address_2']:
                customer_data['address'].append(shipping_data)
        # customer_data['first_name'] = customer_data['first_name'] if customer_data['first_name']: address_data['first_name']
        # customer_data['last_name'] = customer_data['last_name'] if customer_data['last_name']: address_data['last_name']

        # TODO: Plugin WooCommerce Points and Rewards
        if self._notice['src']['support'].get('customer_point_rewards'):
            customer_point_rewards = dict()
            customer_point_rewards['reward_point'] = list()
            customer_point_rewards['reward_point_log'] = list()
            customer_point_rewards['points_balance'] = get_row_value_from_list_by_field(customer_meta, 'meta_key',
                                                                                        'wc_points_balance',
                                                                                        'meta_value')
            wc_points_rewards_user_points = get_list_from_list_by_field(
                customers_ext['data']['wc_points_rewards_user_points'], 'user_id', customer['ID'])
            if wc_points_rewards_user_points:
                for points_rewards_user_points in wc_points_rewards_user_points:
                    reward_point = dict()
                    reward_point['points'] = points_rewards_user_points['points']
                    reward_point['points_balance'] = points_rewards_user_points['points_balance']
                    reward_point['order_id'] = points_rewards_user_points['order_id']
                    reward_point['created_at'] = points_rewards_user_points['date']
                    customer_point_rewards['reward_point'].append(reward_point)
            wc_points_rewards_user_points_log = get_list_from_list_by_field(
                customers_ext['data']['wc_points_rewards_user_points_log'], 'user_id', customer['ID'])
            if wc_points_rewards_user_points_log:
                for points_rewards_user_points_log in wc_points_rewards_user_points_log:
                    reward_point_log = dict()
                    reward_point_log['points'] = points_rewards_user_points_log['points']
                    reward_point_log['type'] = points_rewards_user_points_log['type']
                    reward_point_log['user_points_id'] = points_rewards_user_points_log['user_points_id']
                    reward_point_log['order_id'] = points_rewards_user_points_log['order_id']
                    reward_point_log['admin_user_id'] = points_rewards_user_points_log['admin_user_id']
                    reward_point_log['data'] = points_rewards_user_points_log['data']
                    reward_point_log['created_at'] = points_rewards_user_points_log['date']
                    customer_point_rewards['reward_point_log'].append(reward_point_log)
            customer_data['point_rewards'] = customer_point_rewards
        return response_success(customer_data)

    def get_customer_id_import(self, convert, customer, customers_ext):
        return customer['ID']

    def check_customer_import(self, convert, customer, customers_ext):
        return True if self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['id'], convert['code']) else False

    def router_customer_import(self, convert, customer, customers_ext):
        return response_success('customer_import')

    def before_customer_import(self, convert, customer, customers_ext):
        return response_success()

    def customer_import(self, convert, customer, customers_ext):
        # resource: customer
        self.log(convert, 'convert_customer')

        # insert to users
        users = {
            'user_login': convert['username'],
            'user_pass': convert['password'],
            'user_nicename': convert['username'],
            'user_email': convert['email'],
            'user_url': '',
            'user_registered': convert['created_at'] if convert['created_at'] else get_current_time(),
            'user_activation_key': '',
            'user_status': 0,
            'display_name': convert['first_name'] + ' ' + convert['last_name']
        }
        users_query = self.create_insert_query_connector('users', users)
        users_import_id = self.import_customer_data_connector(users_query, True, convert['id'])
        if not users_import_id:
            return response_warning('users[' + to_str(users) + '] cannot insert !')

        self.insert_map(self.TYPE_CUSTOMER, convert['id'], users_import_id, convert['code'])
        return response_success(users_import_id)

    def after_customer_import(self, customer_id, convert, customer, customers_ext):
        # insert to usermeta
        usermeta_key = {
            'nickname': convert['last_name'] + ' ' + convert['first_name'],
            'first_name': convert['first_name'],
            'last_name': convert['last_name'],
            'description': '',
            'rich_editing': 'true',
            'syntax_highlighting': 'true',
            'comment_shortcuts': 'false',
            'admin_color': 'fresh',
            'use_ssl': 0,
            'show_admin_bar_front': 'true',
            'locale': '',
            'wp_capabilities': 'a:1:{s:8:"customer";b:1;}',
            'wp_user_level': 0,
            'dismissed_wp_pointers': '',
            'last_update': int(round(datetime.fromisoformat(get_current_time()).timestamp())),
            '_order_count': 0,
            'billing_first_name': convert['first_name'],
            'billing_last_name': convert['last_name'],
            'billing_company': convert['address'][0]['company'],
            'billing_address_1': convert['address'][0]['address_1'],
            'billing_address_2': '',
            'billing_city': convert['address'][0]['city'],
            'billing_postcode': convert['address'][0]['postcode'],
            'billing_country': convert['address'][0]['country']['country_code'],
            'billing_state': convert['address'][0]['state']['state_code'],
            'billing_phone': convert['address'][0]['telephone'],
            'billing_email': convert['email'],
            'shipping_first_name': convert['first_name'],
            'shipping_last_name': convert['last_name'],
            'shipping_company': convert['address'][0]['company'],
            'shipping_address_1': convert['address'][0]['address_1'],
            'shipping_address_2': '',
            'shipping_city': convert['address'][0]['city'],
            'shipping_postcode': convert['address'][0]['postcode'],
            'shipping_country': convert['address'][0]['country']['country_code'],
            'shipping_state': convert['address'][0]['state']['state_code'],
            '_last_order': '',
        }

        for key, value in usermeta_key.items():
            usermeta = {
                'user_id': customer_id,
                'meta_key': key,
                'meta_value': value
            }

            usermeta_query = self.create_insert_query_connector('usermeta', usermeta)
            usermeta_import_id = self.import_data_connector(usermeta_query, self.TYPE_CUSTOMER)
            if not usermeta_import_id:
                return response_warning('user[' + to_str(usermeta) + '] cannot insert !')

        # insert to wc_customer_lookup
        wc_customer_lookup = {
            'user_id': customer_id,
            'username': convert['username'],
            'first_name': convert['first_name'],
            'last_name': convert['last_name'],
            'email': convert['email'],
            'date_last_active': get_current_time(),
            'date_registered': convert['created_at'] if convert['created_at'] else get_current_time(),
            'country': convert['address'][0]['country']['country_code'] if convert['address'][0]['country'][
                'country_code'] else 'Vie',
            'postcode': convert['address'][0]['postcode'],
            'city': convert['address'][0]['city'],
            'state': convert['address'][0]['state']['state_code'] if convert['address'][0]['state'][
                'state_code'] else 'VN',
        }

        wc_customer_lookup_query = self.create_insert_query_connector('wc_customer_lookup', wc_customer_lookup)
        wc_customer_lookup_query_import_id = self.import_data_connector(wc_customer_lookup_query, 'customer',
                                                                        convert['id'])
        if not wc_customer_lookup_query_import_id:
            return response_warning('wc_customer_lookup[' + to_str(wc_customer_lookup) + '] cannot insert !')
        return response_success(None, 'Migration data from customer to user complete !')

    def addition_customer_import(self, convert, customer, customers_ext):
        return response_success()

    # TODO: ORDER
    def prepare_orders_import(self):
        return self

    def prepare_orders_export(self):
        return self

    def get_orders_main_export(self):
        id_src = self._notice['process']['orders']['id_src']
        limit = self._notice['setting']['orders']
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_posts WHERE post_type = 'shop_order' AND post_status NOT IN ('inherit','auto-draft') AND ID > " + to_str(
                id_src) + " ORDER BY ID ASC LIMIT " + to_str(limit)
        }
        # orders = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        orders = self.select_data_connector(query, 'orders')
        if not orders or orders['result'] != 'success':
            return response_error()
        return orders

    def get_orders_ext_export(self, orders):
        url_query = self.get_connector_url('query')
        order_ids = duplicate_field_value_from_list(orders['data'], 'ID')
        customer_ext_queries = {
            'woocommerce_order_items': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_woocommerce_order_items WHERE order_id IN  " + self.list_to_in_condition(
                    order_ids),
            },
            'order_note': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_comments WHERE comment_post_ID IN " + self.list_to_in_condition(
                    order_ids),
            },
            'order_refund': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_posts WHERE post_type = 'shop_order_refund' AND post_parent IN " + self.list_to_in_condition(
                    order_ids),
            },
            'order_meta': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_postmeta WHERE post_id IN " + self.list_to_in_condition(order_ids),
            },
        }

        orders_ext = self.select_multiple_data_connector(customer_ext_queries, 'orders')
        if not orders_ext or orders_ext['result'] != 'success':
            return response_error()
        order_item_ids = duplicate_field_value_from_list(orders_ext['data']['woocommerce_order_items'], 'order_item_id')

        comment_ids = duplicate_field_value_from_list(orders_ext['data']['order_note'], 'comment_ID')

        refund_ids = duplicate_field_value_from_list(orders_ext['data']['order_refund'], 'ID')
        post_meta_ids = list(set(refund_ids + order_ids))
        cus_list = get_list_from_list_by_field(orders_ext['data']['order_meta'], 'meta_key', '_customer_user')
        cus_ids = list()
        if cus_list:
            cus_ids = duplicate_field_value_from_list(cus_list, 'meta_value')

        orders_ext_rel_queries = {
            'woocommerce_order_itemmeta': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_woocommerce_order_itemmeta WHERE order_item_id IN " + self.list_to_in_condition(
                    order_item_ids),
            },
            'order_note_meta': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_commentmeta WHERE comment_id IN " + self.list_to_in_condition(
                    comment_ids),
            },
            'postmeta': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_postmeta WHERE post_id IN " + self.list_to_in_condition(post_meta_ids),
            },
            'user': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_users WHERE ID IN " + self.list_to_in_condition(cus_ids),
            },
            'user_meta': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_usermeta WHERE meta_key IN ('first_name','last_name') AND user_id IN  " + self.list_to_in_condition(
                    cus_ids),
            }
        }

        if self._notice['src']['support'].get('customer_point_rewards'):
            orders_ext_rel_queries['wc_points_rewards_user_points'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_wc_points_rewards_user_points WHERE order_id IN " + self.list_to_in_condition(
                    order_ids),
            }
            orders_ext_rel_queries['wc_points_rewards_user_points_log'] = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_wc_points_rewards_user_points_log WHERE order_id IN " + self.list_to_in_condition(
                    order_ids),
            }

        orders_ext_rel = self.select_multiple_data_connector(orders_ext_rel_queries, 'orders')
        if not orders_ext_rel or orders_ext_rel['result'] != 'success':
            return response_error()
        orders_ext = self.sync_connector_object(orders_ext, orders_ext_rel)

        pro_list = get_list_from_list_by_field(orders_ext_rel['data']['woocommerce_order_itemmeta'], 'meta_key',
                                               '_product_id')
        pro_ids = duplicate_field_value_from_list(pro_list, 'meta_value')

        orders_ext_third_rel_queries = {
            'products_meta': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_postmeta WHERE post_id IN " + self.list_to_in_condition(pro_ids),
            },
        }
        orders_ext_third_rel = self.get_connector_data(url_query, {'serialize': True,
                                                                   'query': json.dumps(orders_ext_third_rel_queries)})
        if not orders_ext_third_rel or orders_ext_third_rel['result'] != 'success':
            return response_error()
        orders_ext = self.sync_connector_object(orders_ext, orders_ext_third_rel)
        return orders_ext

    def convert_order_export(self, order, orders_ext):
        order_data = self.construct_order()
        order_data = self.add_construct_default(order_data)
        order_data['id'] = order['ID']
        order_data['status'] = order['post_status']
        # order data
        order_items = get_list_from_list_by_field(orders_ext['data']['woocommerce_order_items'], 'order_id',
                                                  order['ID'])
        shipping = get_row_from_list_by_field(order_items, 'order_item_type', 'shipping')
        taxes = get_list_from_list_by_field(order_items, 'order_item_type', 'tax')
        tax_names = list()
        total_tax = 0.0
        if taxes:
            tax_names = duplicate_field_value_from_list(taxes, 'order_item_name')

            for tax in taxes:
                order_tax_metas = get_list_from_list_by_field(orders_ext['data']['woocommerce_order_itemmeta'],
                                                              'order_item_id', tax['order_item_id'])
                total_tax += to_decimal(self.get_value_metadata(order_tax_metas, 'tax_amount', 0.0))
                total_tax += to_decimal(self.get_value_metadata(order_tax_metas, 'shipping_tax_amount', 0.0))
        if 'postmeta' in orders_ext['data']:
            order_meta = get_list_from_list_by_field(orders_ext['data']['postmeta'], 'post_id', order['ID'])
        else:
            order_meta = get_list_from_list_by_field(orders_ext['data']['order_meta'], 'post_id', order['ID'])
        ord_number = get_row_value_from_list_by_field(order_meta, 'meta_key', '_order_number', 'meta_value')
        if ord_number and self._notice['src']['support'].get('plugin_pre_ord'):
            order_data['order_number'] = ord_number
        order_data['tax']['title'] = '|'.join(tax_names) if tax_names else 'Tax'
        order_data['tax']['amount'] = total_tax if total_tax else self.get_value_metadata(order_meta, '_order_tax',
                                                                                          0.0000)
        order_data['shipping']['title'] = shipping['order_item_name'] if shipping else 'Shipping'
        order_data['shipping']['amount'] = self.get_value_metadata(order_meta, '_order_shipping',
                                                                   0.0000)  # _order_shipping_tax
        discount_title = get_row_value_from_list_by_field(order_items, 'order_item_type', 'coupon', 'order_item_name')
        order_data['discount']['title'] = discount_title if discount_title else 'Discount'
        order_data['discount']['amount'] = self.get_value_metadata(order_meta, '_cart_discount', 0.0000)
        order_data['total']['title'] = 'Total'
        order_data['total']['amount'] = self.get_value_metadata(order_meta, '_order_total', 0.0000)
        order_data['subtotal']['title'] = 'Total'
        order_data['subtotal']['amount'] = to_decimal(
            self.get_value_metadata(order_meta, '_order_total', 0.0000)) - to_decimal(
            self.get_value_metadata(order_meta, '_cart_discount', 0.0000)) - to_decimal(
            order_data['tax']['amount']) - to_decimal(order_data['shipping']['amount'])
        order_data['currency'] = self.get_value_metadata(order_meta, '_order_currency', 'meta_value')
        order_data['created_at'] = convert_format_time(order['post_date'])
        order_data['updated_at'] = convert_format_time(order['post_modified'])
        # order customer
        order_customer = self.construct_order_customer()
        order_customer = self.add_construct_default(order_customer)
        order_customer_src = self.get_value_metadata(order_meta, '_customer_user', 'meta_value')
        if order_customer_src and to_int(order_customer_src) > 0:
            customer_src = get_row_from_list_by_field(orders_ext['data']['user'], 'ID', order_customer_src)
            customer_meta_src = get_list_from_list_by_field(orders_ext['data']['user_meta'], 'user_id',
                                                            order_customer_src)
            if customer_src:
                order_customer['id'] = order_customer_src
                order_customer['code'] = get_value_by_key_in_dict(customer_src, 'user_login', '')
                order_customer['email'] = get_value_by_key_in_dict(customer_src, 'user_email',
                                                                   self.get_value_metadata(order_meta, '_billing_email',
                                                                                           'meta_value'))
                order_customer['username'] = get_value_by_key_in_dict(customer_src, 'display_name', '')
            order_customer['first_name'] = self.get_value_metadata(customer_meta_src, 'first_name',
                                                                   self.get_value_metadata(order_meta,
                                                                                           '_billing_first_name', ''))
            order_customer['last_name'] = self.get_value_metadata(customer_meta_src, 'last_name',
                                                                  self.get_value_metadata(order_meta,
                                                                                          '_billing_last_name', ''))
        else:
            order_customer['email'] = self.get_value_metadata(order_meta, '_billing_email', 'meta_value')
            order_customer['username'] = order_customer['email']
            order_customer['first_name'] = self.get_value_metadata(order_meta, '_billing_first_name', '')
            order_customer['last_name'] = self.get_value_metadata(order_meta, '_billing_last_name', '')
        order_data['customer'] = order_customer

        # TODO: Plugin WooCommerce Points and Rewards
        if self._notice['src']['support'].get('customer_point_rewards'):
            customer_point_rewards = dict()
            customer_point_rewards['reward_point'] = list()
            customer_point_rewards['reward_point_log'] = list()
            wc_points_rewards_user_points = get_list_from_list_by_field(
                orders_ext['data']['wc_points_rewards_user_points'], 'order_id', order['ID'])
            if wc_points_rewards_user_points:
                for points_rewards_user_points in wc_points_rewards_user_points:
                    reward_point = dict()
                    reward_point['points'] = points_rewards_user_points['points']
                    reward_point['points_balance'] = points_rewards_user_points['points_balance']
                    reward_point['user_id'] = points_rewards_user_points['user_id']
                    reward_point['created_at'] = points_rewards_user_points['date']
                    customer_point_rewards['reward_point'].append(reward_point)
            wc_points_rewards_user_points_log = get_list_from_list_by_field(
                orders_ext['data']['wc_points_rewards_user_points_log'], 'order_id', order['ID'])
            if wc_points_rewards_user_points_log:
                for points_rewards_user_points_log in wc_points_rewards_user_points_log:
                    reward_point_log = dict()
                    reward_point_log['points'] = points_rewards_user_points_log['points']
                    reward_point_log['type'] = points_rewards_user_points_log['type']
                    reward_point_log['user_points_id'] = points_rewards_user_points_log['user_points_id']
                    reward_point_log['user_id'] = points_rewards_user_points_log['user_id']
                    reward_point_log['admin_user_id'] = points_rewards_user_points_log['admin_user_id']
                    reward_point_log['data'] = points_rewards_user_points_log['data']
                    reward_point_log['created_at'] = points_rewards_user_points_log['date']
                    customer_point_rewards['reward_point_log'].append(reward_point_log)
            order_data['point_rewards'] = customer_point_rewards

        # customer address
        customer_address = self.construct_order_address()
        customer_address = self.add_construct_default(customer_address)
        customer_address['first_name'] = self.get_value_metadata(order_meta, '_billing_first_name', '')
        customer_address['last_name'] = self.get_value_metadata(order_meta, '_billing_last_name', '')
        customer_address['email'] = self.get_value_metadata(order_meta, '_billing_email', '')
        customer_address['address_1'] = self.get_value_metadata(order_meta, '_billing_address_1', '')
        customer_address['address_2'] = self.get_value_metadata(order_meta, '_billing_address_2', '')
        customer_address['city'] = self.get_value_metadata(order_meta, '_billing_city', '')
        customer_address['postcode'] = self.get_value_metadata(order_meta, '_billing_postcode', '')
        customer_address['telephone'] = self.get_value_metadata(order_meta, '_billing_phone', '')
        customer_address['company'] = self.get_value_metadata(order_meta, '_billing_company', '')
        customer_address['country']['code'] = self.get_value_metadata(order_meta, '_billing_country', '')
        customer_address['country']['country_code'] = self.get_value_metadata(order_meta, '_billing_country', '')
        customer_address['country']['name'] = self.get_country_name_by_code(customer_address['country']['country_code'])
        customer_address['state']['state_code'] = self.get_value_metadata(order_meta, '_billing_state', '')
        customer_address['state']['code'] = customer_address['state']['state_code']
        order_data['customer_address'] = customer_address
        # billing address
        order_billing = self.construct_order_address()
        order_billing = self.add_construct_default(order_billing)
        order_billing['first_name'] = self.get_value_metadata(order_meta, '_billing_first_name', '')
        order_billing['last_name'] = self.get_value_metadata(order_meta, '_billing_last_name', '')
        order_billing['email'] = self.get_value_metadata(order_meta, '_billing_email', '')
        order_billing['address_1'] = self.get_value_metadata(order_meta, '_billing_address_1', '')
        order_billing['address_2'] = self.get_value_metadata(order_meta, '_billing_address_2', '')
        order_billing['city'] = self.get_value_metadata(order_meta, '_billing_city', '')
        order_billing['postcode'] = self.get_value_metadata(order_meta, '_billing_postcode', '')
        order_billing['telephone'] = self.get_value_metadata(order_meta, '_billing_phone', '')
        order_billing['company'] = self.get_value_metadata(order_meta, '_billing_company', '')
        order_billing['country']['code'] = self.get_value_metadata(order_meta, '_billing_country', '')
        order_billing['country']['country_code'] = self.get_value_metadata(order_meta, '_billing_country', '')
        order_billing['country']['name'] = self.get_country_name_by_code(order_billing['country']['country_code'])
        order_billing['state']['state_code'] = self.get_value_metadata(order_meta, '_billing_state', '')
        order_billing['state']['code'] = order_billing['state']['state_code']
        order_billing['code'] = self.convert_attribute_code(
            to_str(order_billing['first_name']) + '-' + to_str(order_billing['last_name']) + '-' + to_str(
                order_billing['address_1']) + '-' + to_str(order_billing['address_2']))
        order_data['billing_address'] = order_billing
        # shipping address
        order_delivery = self.construct_order_address()
        order_delivery = self.add_construct_default(order_delivery)
        order_delivery['first_name'] = self.get_value_metadata(order_meta, '_shipping_first_name', '')
        order_delivery['last_name'] = self.get_value_metadata(order_meta, '_shipping_last_name', '')
        order_delivery['email'] = self.get_value_metadata(order_meta, '_shipping_email', '')
        order_delivery['address_1'] = self.get_value_metadata(order_meta, '_shipping_address_1', '')
        order_delivery['address_2'] = self.get_value_metadata(order_meta, '_shipping_address_2', '')
        order_delivery['city'] = self.get_value_metadata(order_meta, '_shipping_city', '')
        order_delivery['postcode'] = self.get_value_metadata(order_meta, '_shipping_postcode', '')
        order_delivery['telephone'] = self.get_value_metadata(order_meta, '_shipping_phone',
                                                              '') if self.get_value_metadata(order_meta,
                                                                                             '_shipping_phone',
                                                                                             '') else self.get_value_metadata(
            order_meta, '_shipping_Phone_No', '')
        order_delivery['company'] = self.get_value_metadata(order_meta, '_shipping_company', '')
        order_delivery['country']['code'] = self.get_value_metadata(order_meta, '_shipping_country', '')
        order_delivery['country']['country_code'] = self.get_value_metadata(order_meta, '_shipping_country', '')
        order_delivery['country']['name'] = self.get_country_name_by_code(order_delivery['country']['country_code'])
        order_delivery['state']['state_code'] = self.get_value_metadata(order_meta, '_shipping_state', '')
        order_delivery['state']['code'] = order_delivery['state']['state_code']
        order_delivery['code'] = self.convert_attribute_code(
            to_str(order_delivery['first_name']) + '-' + to_str(order_delivery['last_name']) + '-' + to_str(
                order_delivery['address_1']) + '-' + to_str(order_delivery['address_2']))
        order_data['shipping_address'] = order_delivery

        order_products = get_list_from_list_by_field(order_items, 'order_item_type', 'line_item')
        order_items = list()
        for order_product in order_products:
            order_product_metas = get_list_from_list_by_field(orders_ext['data']['woocommerce_order_itemmeta'],
                                                              'order_item_id', order_product['order_item_id'])
            qty = self.get_value_metadata(order_product_metas, '_qty', 1)
            if to_int(qty) == 0:
                qty = 1
            order_item_subtotal = self.get_value_metadata(order_product_metas, '_line_subtotal', 0.0000)

            order_item = self.construct_order_item()
            order_item = self.add_construct_default(order_item)
            order_item['id'] = order_product['order_item_id']
            order_item['product']['id'] = self.get_value_metadata(order_product_metas, '_variation_id',
                                                                  self.get_value_metadata(order_product_metas,
                                                                                          '_product_id', 0))
            order_item['product']['code'] = self.get_value_metadata(order_product_metas, '_product_code', 0)
            product_meta = get_list_from_list_by_field(orders_ext['data']['products_meta'], 'post_id',
                                                       order_item['product']['id'])
            order_item['product']['sku'] = self.get_value_metadata(product_meta, '_sku', '')
            order_item['product']['name'] = order_product['order_item_name']
            order_item['qty'] = to_decimal(qty) if qty != '' else 1
            order_item['price'] = to_decimal(order_item_subtotal) / to_decimal(qty) if (qty != 0 and qty != '') else 0
            order_item['original_price'] = to_decimal(order_item_subtotal) / to_decimal(qty) if (
                    qty != 0 and qty != '') else 0
            order_item['tax_amount'] = self.get_value_metadata(order_product_metas, '_line_tax', 0.0000)
            order_item['subtotal'] = order_item_subtotal
            order_item['total'] = self.get_value_metadata(order_product_metas, '_line_total', 0.0000)
            order_item['options'] = list()
            if order_product['order_item_type'] == 'line_item':
                order_item_options = list()
                keys = {'_qty', '_tax_class', '_product_id', '_variation_id', '_line_subtotal', '_line_subtotal_tax',
                        '_line_total', '_line_tax', '_line_tax_data', '_original_order_item_id'}
                for order_product_meta in order_product_metas:
                    if order_product_meta['meta_key'] not in keys:
                        order_item_option = self.construct_order_item_option()
                        # order_item_option['option_name'] = order_product_meta['meta_key']
                        order_item_option['option_name'] = unquote(order_product_meta['meta_key'])
                        if order_item_option['option_name'] and 'pa_' in order_item_option['option_name']:
                            continue
                        order_item_option['option_value_name'] = order_product_meta['meta_value']
                        # unquote(order_product['order_item_name'])
                        order_item_options.append(order_item_option)
                order_item['options'] = order_item_options

            order_items.append(order_item)

        order_data['items'] = order_items

        order_notes = get_list_from_list_by_field(orders_ext['data']['order_note'], 'comment_post_ID', order['ID'])
        order_history = list()
        for order_note in order_notes:
            order_note_meta = get_list_from_list_by_field(orders_ext['data']['order_note_meta'], 'comment_id',
                                                          order_note['comment_ID'])
            order_history = self.construct_order_history()
            order_history = self.add_construct_default(order_history)
            order_history['id'] = order_note['comment_ID']
            order_history['status'] = order_note['comment_approved']
            order_history['comment'] = order_note['comment_content']
            order_history['notified'] = self.get_value_metadata(order_note_meta, 'is_customer_note', False)
            order_history['created_at'] = convert_format_time(order_note['comment_date'])

            order_data['history'].append(order_history)

        order_payment = self.construct_order_payment()
        order_payment = self.add_construct_default(order_payment)
        order_payment['id'] = order['ID']
        order_payment['method'] = self.get_value_metadata(order_meta, '_payment_method')
        order_payment['title'] = self.get_value_metadata(order_meta, '_payment_method_title')
        order_data['payment'] = order_payment

        return response_success(order_data)

    def get_order_id_import(self, convert, order, orders_ext):
        return order['ID']

    def check_order_import(self, convert, order, orders_ext):
        return self.get_map_field_by_src(self.TYPE_ORDER, convert['id'], convert['code'])

    def update_order_after_demo(self, order_id, convert, order, orders_ext):
        all_queries = list()
        delete_query = list()
        # order item
        delete_query_child = {
            'type': 'delete',
            'query': 'DELETE FROM _DBPRF_woocommerce_order_itemmeta WHERE order_item_id IN (SELECT order_item_id FROM _DBPFF_woocommerce_order_items WHERE order_id = ' + to_str(
                order_id) + ')'
        }
        delete_query.append(delete_query_child)
        delete_query.append(self.create_delete_query_connector('woocommerce_order_items', {'order_id': order_id}))
        self.import_multiple_data_connector(delete_query, 'delete_ord_update')
        order_items = convert['items']
        for item in order_items:
            order_item_data = {
                'order_item_name': item['product']['name'],
                'order_item_type': 'line_item',
                'order_id': order_id
            }
            order_item_query = self.create_insert_query_connector("woocommerce_order_items", order_item_data)
            order_item_id = self.import_data_connector(order_item_query, 'order')
            product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, item['product']['id'])
            if not product_id:
                product_id = self.get_map_field_by_src(self.TYPE_PRODUCT, None, item['product']['id'])
            if not product_id:
                product_id = 0

            order_item_meta = {
                '_qty': item['qty'],
                '_tax_class': '',
                '_product_id': product_id,
                '_variation_id': '',
                '_line_subtotal': item['subtotal'],
                '_line_total': item['total'],
                '_line_subtotal_tax': 0,
                '_line_tax': 0,
                '_line_tax_data': php_serialize({
                    'total': 0,
                    'subtotal': 0
                }),
            }
            for meta_key, meta_value in order_item_meta.items():
                meta_insert = {
                    'order_item_id': order_item_id,
                    'meta_key': meta_key,
                    'meta_value': meta_value
                }
                meta_query = self.create_insert_query_connector("woocommerce_order_itemmeta", meta_insert)
                all_queries.append(meta_query)
            for option in item['options']:
                meta_insert = {
                    'order_item_id': order_item_id,
                    'meta_key': option['option_name'],
                    'meta_value': option['option_value_name']
                }
                meta_query = self.create_insert_query_connector("woocommerce_order_itemmeta", meta_insert)
                all_queries.append(meta_query)
        return response_success()

    def router_order_import(self, convert, order, orders_ext):
        return response_success('order_import')

    def before_order_import(self, convert, order, orders_ext):
        return response_success()

    def order_import(self, convert, order, orders_ext):
        # resource: orders
        self.log(convert, 'convert_orders')

        # insert to posts [type: order]
        create_at = convert['created_at']
        posts_order = {
            'post_author': 1,
            'post_date': create_at if create_at else get_current_time(),
            'post_date_gmt': create_at if create_at else get_current_time(),
            'post_content': '',
            'post_title': datetime.fromisoformat(create_at).strftime('Order &ndash; %B %d, %Y @ %0l:%M %p'),
            'post_excerpt': '',
            'post_status': 'wc-pending',
            'comment_status': 'closed',
            'ping_status': 'closed',
            'post_password': '',
            'post_name': '',
            'to_ping': '',
            'pinged': '',
            'post_modified': convert['updated_at'] if convert['updated_at'] else get_current_time(),
            'post_modified_gmt': convert['updated_at'] if convert['updated_at'] else get_current_time(),
            'post_content_filtered': '',
            'post_parent': 0,
            'guid': '',
            'menu_order': 0,
            'post_type': 'shop_order',
            'post_mime_type': '',
            'comment_count': 0
        }

        posts_order_query = self.create_insert_query_connector('posts', posts_order)
        posts_order_import_id = self.import_data_connector(posts_order_query, self.TYPE_ORDER, convert['id'])
        if not posts_order_import_id:
            return response_warning('order[' + to_str(posts_order) + '] cannot insert !')

        posts_update = {
            'guid': 'http://localhost/wordpress/?post_type=shop_order&#038;p=' + to_str(posts_order_import_id)
        }
        update_query = self.create_update_query_connector('posts', posts_update, {'ID': posts_order_import_id})
        update_post_id = self.import_data_connector(update_query, self.TYPE_ORDER, posts_order_import_id)
        if not update_post_id:
            return response_warning('query: ' + to_str(update_query) + ' cannot execute !')

        self.insert_map(self.TYPE_ORDER, convert['id'], posts_order_import_id, convert['code'])
        return response_success(posts_order_import_id)

    def after_order_import(self, order_id, convert, order, orders_ext):
        # variable child
        customer = convert['customer']
        customer_address = convert['customer_address']
        billing_address = convert['billing_address']
        shipping_address = convert['shipping_address']

        # insert to postmeta
        order_customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, customer['id'])
        _billing_address_index = billing_address['first_name'] + '-' \
                                 + billing_address['last_name'] + '-' \
                                 + billing_address['company'] + '-' \
                                 + billing_address['address_1'] + '-' \
                                 + billing_address['address_2'] + '-' + billing_address['city'] + '-' \
                                 + billing_address['state']['state_code'] + '-' \
                                 + convert['customer']['email'] + '-' \
                                 + billing_address['telephone']
        _shipping_address_index = shipping_address['first_name'] + '-' \
                                  + shipping_address['last_name'] + '-' \
                                  + shipping_address['company'] + '-' \
                                  + shipping_address['address_1'] + '-' \
                                  + shipping_address['address_2'] + '-' \
                                  + shipping_address['city'] + '-' \
                                  + shipping_address['state']['state_code'] + '-' \
                                  + shipping_address['postcode']
        postmeta_order_key = {
            '_edit_lock': to_str(int(time.time())) + ':1',
            '_edit_last': '1',
            '_customer_user': order_customer_id if order_customer_id else 0,
            '_order_currency': convert['currency'],
            '_order_shipping_tax': 0,
            '_order_tax': convert['tax']['amount'],
            '_order_total': convert['total']['amount'],
            '_order_version': '4.9.0',
            '_prices_include_tax': 'no',
            '_billing_address_index': _billing_address_index,
            '_shipping_address_index': _shipping_address_index,
            '_cart_discount': 0,
            '_cart_discount_tax': 0,
            '_order_shipping': convert['shipping']['amount'],
            '_order_key': 'wc_order_4ldQfbvtHNLzl',
            '_create_via': 'admin',
            '_billing_first_name': billing_address['first_name'],
            '_billing_last_name': billing_address['last_name'],
            '_billing_company': billing_address['company'],
            '_billing_address_1': billing_address['address_1'],
            '_billing_address_2': billing_address['address_2'],
            '_billing_city': billing_address['city'],
            '_billing_state': billing_address['state']['state_code'],
            '_billing_postcode': billing_address['postcode'],
            '_billing_country': billing_address['country']['country_code'],
            '_billing_email': convert['customer']['email'],
            '_billing_phone': billing_address['telephone'],
            '_shipping_first_name': shipping_address['first_name'],
            '_shipping_last_name': shipping_address['last_name'],
            '_shipping_company': shipping_address['company'],
            '_shipping_address_1': shipping_address['address_1'],
            '_shipping_address_2': shipping_address['address_2'],
            '_shipping_city': shipping_address['city'],
            '_shipping_state': shipping_address['state']['state_code'],
            '_shipping_postcode': shipping_address['postcode'],
            '_shipping_country': shipping_address['country']['country_code']
        }

        for key, value in postmeta_order_key.items():
            order_postmeta_data = {
                'post_id': order_id,
                'meta_key': key,
                'meta_value': value
            }
            postmeta_order_query = self.create_insert_query_connector('postmeta', order_postmeta_data)
            postmeta_order_import_id = self.import_data_connector(postmeta_order_query, self.TYPE_ORDER, order_id)
            if not postmeta_order_import_id:
                return response_warning('order_postmeta[' + to_str(order_id) + '] cannot insert !')

        # insert to multiple woocommerce_order table
        for product in convert['items']:
            product_id = 0
            product_map_id = self.get_map_field_by_src(self.TYPE_PRODUCT, product['id'])
            if product_map_id:
                product_id = product_map_id

            # insert to woocommerce_order_items
            woocommerce_order_items = {
                'order_item_name': product['product']['name'],
                'order_item_type': 'line_item',
                'order_id': order_id
            }
            woocommerce_order_items_query = self.create_insert_query_connector('woocommerce_order_items',
                                                                               woocommerce_order_items)
            woocommerce_order_items_import_id = self.import_data_connector(woocommerce_order_items_query,
                                                                           self.TYPE_ORDER, order_id)
            if not woocommerce_order_items_import_id:
                return response_warning(
                    'woocommerce_order_items[' + to_str(woocommerce_order_items) + '] cannot insert !')

            # insert to woocommerce_order_itemmeta
            woocommerce_order_itemmeta_meta_key = {
                '_product_id': product_id,
                '_variation_id': 0,
                '_qty': product['qty'],
                '_tax_class': '',
                '_line_subtotal': product['subtotal'],
                '_line_subtotal_tax': 0,
                '_line_total': product['total'],
                '_line_tax': 0,
                '_line_tax_data': 'a:2:{s:8:"subtotal";a:0:{}s:5:"total";a:0:{}}'
            }

            woocommerce_order_itemmeta = {
                'order_item_id': woocommerce_order_items_import_id,
                'meta_key': '',
                'meta_value': ''
            }

            for key, value in woocommerce_order_itemmeta_meta_key.items():
                woocommerce_order_itemmeta['meta_key'] = key
                woocommerce_order_itemmeta['meta_value'] = value

                woocommerce_order_itemmeta_query = self.create_insert_query_connector(
                    'woocommerce_order_itemmeta', woocommerce_order_itemmeta)
                woocommerce_order_itemmeta_import_id = self.import_data_connector(
                    woocommerce_order_itemmeta_query, self.TYPE_ORDER, woocommerce_order_items_import_id)
                if not woocommerce_order_itemmeta_import_id:
                    return response_warning('woocommerce_order_itemmeta['
                                            + to_str(woocommerce_order_itemmeta) + '] cannot insert !')

            # insert to wc_order_product_lookup
            wc_order_product_lookup = {
                'order_item_id': woocommerce_order_items_import_id,
                'order_id': order_id,
                'product_id': product_id,
                'variation_id': 0,
                'customer_id': order_customer_id if order_customer_id else 0,
                'date_created': product['created_at'] if product['created_at'] else product['updated_at'],
                'product_qty': product['qty'],
                'product_net_revenue': 0.0,
                'product_gross_revenue': 0.0,
                'coupon_amount': 0.0,
                'tax_amount': 0.0,
                'shipping_amount': float(convert['shipping']['amount']) / len(convert['items']),
                'shipping_tax_amount': 0.0
            }
            wc_order_product_lookup_query = self.create_insert_query_connector('wc_order_product_lookup',
                                                                               wc_order_product_lookup)
            wc_order_product_lookup_import_id = self.import_data_connector(wc_order_product_lookup_query,
                                                                           self.TYPE_ORDER, order_id)
            if not wc_order_product_lookup_import_id:
                return response_warning(
                    'wc_order_product_lookup[' + to_str(wc_order_product_lookup) + '] cannot import !')

        # insert shipping type to woocommerce_order_items
        woocommerce_order_items_shipping = {
            'order_item_name': 'Woocommerce Order Items Shipping',
            'order_item_type': 'shipping',
            'order_id': order_id
        }
        woocommerce_order_items_shipping_query = self.create_insert_query_connector('woocommerce_order_items',
                                                                                    woocommerce_order_items_shipping)
        woocommerce_order_items_shipping_import_id = self.import_data_connector(woocommerce_order_items_shipping_query,
                                                                                self.TYPE_ORDER, order_id)
        if not woocommerce_order_items_shipping_import_id:
            return response_warning(
                'type: shipping woocommerce_order_items_shipping[' + to_str(
                    woocommerce_order_items_shipping) + '] cannot insert !')

        # insert shipping type woocommerce_order_itemmeta
        woocommerce_order_itemmeta_shipping_key = {
            'method_id': '',
            'instance_id': 0,
            'cost': convert['shipping']['amount'],
            'total_tax': 0,
            'taxes': 'a:1:{s:5:"total";a:0:{}}'
        }

        woocommerce_order_itemmeta_shipping = {
            'order_item_id': woocommerce_order_items_shipping_import_id,
            'meta_key': '',
            'meta_value': '',
        }

        for key, value in woocommerce_order_itemmeta_shipping_key.items():
            woocommerce_order_itemmeta_shipping['meta_key'] = key
            woocommerce_order_itemmeta_shipping['meta_value'] = value

            woocommerce_order_itemmeta_shipping_query = self.create_insert_query_connector('woocommerce_order_itemmeta',
                                                                                           woocommerce_order_itemmeta_shipping)
            woocommerce_order_itemmeta_shipping_import = self.import_data_connector(
                woocommerce_order_itemmeta_shipping_query, self.TYPE_ORDER, woocommerce_order_items_shipping_import_id)
            if not woocommerce_order_itemmeta_shipping_import:
                return response_warning('type: shipping woocommerce_order_itemmeta['
                                        + to_str(woocommerce_order_itemmeta_shipping)
                                        + ' cannot insert !')

        # insert to wc_order_stats
        wc_order_stats = {
            'order_id': order_id,
            'parent_id': 0,
            'date_created': convert['created_at'] if convert['created_at'] else get_current_time(),
            'date_created_gmt': convert['created_at'] if convert['created_at'] else get_current_time(),
            'num_items_sold': len(convert['items']),
            'total_sales': convert['total']['amount'],
            'tax_total': convert['tax']['amount'],
            'shipping_total': convert['shipping']['amount'],
            'net_total': convert['total']['amount'],
            'returning_customer': 0,
            'status': 'wc-pending',
            'customer_id': order_customer_id if order_customer_id else 0
        }

        wc_order_stats_query = self.create_insert_query_connector('wc_order_stats', wc_order_stats)
        wc_order_stats_import_id = self.import_data_connector(wc_order_stats_query, self.TYPE_ORDER, order_id)
        if not wc_order_stats_import_id:
            return response_warning('wc_order_stats[' + to_str(wc_order_stats) + '] cannot insert !')

        return response_success()

    def addition_order_import(self, convert, order, orders_ext):
        return response_success()

    # TODO: Template
    def display_finish_target(self):
        migration_id = self._migration_id
        recent_exist = self.select_row(TABLE_RECENT, {'migration_id': migration_id})
        notice = json.dumps(self._notice)
        if recent_exist:
            self.update_obj(TABLE_RECENT, {'notice': notice}, {'migration_id': migration_id})
        else:
            self.insert_obj(TABLE_RECENT, {'notice': notice, 'migration_id': migration_id})
        target_cart_type = self._notice['target']['cart_type']
        target_setup_type = self.target_cart_setup(target_cart_type)
        # if target_setup_type == 'connector':
        token = self._notice['target']['config']['token']
        url = self.get_connector_url('clearcache', token)
        self.get_connector_data(url)
        all_queries = list()
        all_queries.append({
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_options` WHERE option_name = 'product_cat_children'"
        })
        all_queries.append({
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_options` WHERE option_name = '_transient_wc_attribute_taxonomies'"
        })
        all_queries.append({
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_options` WHERE `option_name` LIKE '%_transient_timeout_wc_report_customers%'"
        })
        all_queries.append({
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_options` WHERE `option_name` LIKE '%_transient_wc_report_customers%'"
        })
        all_queries.append({
            'type': 'query',
            'query': "DELETE FROM `_DBPRF_options` WHERE option_name = 'urlrewrite_type'"
        })
        all_queries.append({
            'type': 'query',
            'query': "UPDATE `_DBPRF_posts` SET `comment_count`= (SELECT COUNT(comment_ID) FROM `_DBPRF_comments` WHERE `_DBPRF_comments`.comment_post_ID = `_DBPRF_posts`.ID AND `_DBPRF_comments`.comment_approved = 1) WHERE `post_type` IN ('product', 'post')"
        })
        all_queries.append({
            'type': 'query',
            'query': "UPDATE `_DBPRF_postmeta` SET `meta_value`= (SELECT COUNT(comment_ID) FROM `_DBPRF_comments` WHERE `_DBPRF_comments`.comment_post_ID = `_DBPRF_postmeta`.post_id AND `_DBPRF_comments`.comment_approved = 1) WHERE `meta_key` = '_wc_review_count'"
        })
        all_queries.append({
            'type': 'query',
            'query': "UPDATE `_DBPRF_postmeta` SET `meta_value`= (SELECT AVG(cmta.`meta_value`) FROM `_DBPRF_comments` AS cmt LEFT JOIN `_DBPRF_commentmeta` AS cmta ON cmt.`comment_ID` = cmta.`comment_ID` WHERE cmt.`comment_post_ID` = `_DBPRF_postmeta`.`post_id` AND cmt.comment_approved = 1 AND cmta.`meta_key` = 'rating') WHERE `meta_key` = '_wc_average_rating'"
        })
        all_queries.append({
            'type': 'query',
            'query': "UPDATE `_DBPRF_term_taxonomy` AS tt SET tt.count = (SELECT COUNT(1) AS total FROM _DBPRF_term_relationships AS tr WHERE tt.term_taxonomy_id = tr.term_taxonomy_id AND tr.object_id IN (SELECT ID FROM _DBPRF_posts WHERE post_type = 'product'))"
        })
        clear_cache = self.import_multiple_data_connector(all_queries)
        option_data = {
            'option_name': 'urlrewrite_type',
            'option_value': 'urlrewrite',
            'autoload': 'yes'
        }
        if self._notice['support'].get('seo_301'):
            option_data = {
                'option_name': 'urlrewrite_type',
                'option_value': 'url301',
                'autoload': 'yes'
            }
        option_query = self.create_insert_query_connector('options', option_data)
        option_import = self.import_data_connector(option_query, 'options')
        return response_success()

    def substr_replace(self, subject, replace, start, length):
        if length == None:
            return subject[:start] + replace
        elif length < 0:
            return subject[:start] + replace + subject[length:]
        else:
            return subject[:start] + replace + subject[start + length:]

    def add_construct_default(self, construct):
        construct['site_id'] = 1
        construct['language_id'] = self._notice['src']['language_default']
        return construct

    def get_term_by_name(self, data):
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_term_taxonomy AS tt "
                     "LEFT JOIN _DBPRF_terms AS t ON t.term_id = tt.term_id "
                     "WHERE tt.taxonomy = 'product_visibility' AND t.name = '" + data + "'"
        }
        product_taxonomy = self.select_data_connector(query)
        if product_taxonomy['result'] == 'success' and product_taxonomy['data']:
            return product_taxonomy['data'][0]['term_taxonomy_id']
        return None

    def get_product_type(self, product_type):
        if not self.product_types:
            query = {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_term_taxonomy AS tt "
                         "LEFT JOIN _DBPRF_terms AS t ON t.term_id = tt.term_id "
                         "WHERE tt.taxonomy = 'product_type'"
            }
            product_types = self.select_data_connector(query)
            if product_types['result'] == 'success' and product_types['data']:
                for product_type_row in product_types['data']:
                    self.product_types[product_type_row['slug']] = product_type_row['term_taxonomy_id']

        return self.product_types.get(product_type, 2)

    def import_category_parent(self, convert_parent, lang_code=None):
        category_type = self.TYPE_CATEGORY
        if convert_parent.get('is_blog'):
            category_type = self.TYPE_CATEGORY_BLOG
        parent_exists = self.get_map_field_by_src(category_type, convert_parent['id'], convert_parent['code'],
                                                  lang_code)
        if parent_exists:
            return response_success(parent_exists)
        if self.is_wpml() and lang_code:
            convert_parent['language_code'] = lang_code
            for src_language_id, target_language_id in self._notice['map']['languages'].items():
                if to_str(lang_code) == to_str(target_language_id):
                    lang_data = convert_parent
                    if to_str(src_language_id) in convert_parent['languages'] and convert_parent['languages'][
                        to_str(src_language_id)]:
                        lang_data = convert_parent['languages'][to_str(src_language_id)]
                    convert_parent['name'] = lang_data['name']
                    convert_parent['description'] = lang_data['description']
                    convert_parent['short_description'] = lang_data['short_description']
                    convert_parent['meta_title'] = lang_data['meta_title']
                    convert_parent['meta_keyword'] = lang_data['meta_keyword']
                    convert_parent['meta_description'] = lang_data['meta_description']
                    convert_parent['url_key'] = lang_data.get('url_key', '')
        category = get_value_by_key_in_dict(convert_parent, 'category', dict())
        categories_ext = get_value_by_key_in_dict(convert_parent, 'categories_ext', dict())
        category_parent_import = self.category_import(convert_parent, category, categories_ext)
        self.after_category_import(category_parent_import['data'], convert_parent, category, categories_ext)
        return category_parent_import

    def get_list_from_list_by_field_as_first_key(self, list_data, field='', first_key=''):
        result = list()
        if isinstance(list_data, dict):
            for key, row in list_data.items():
                if field in row:
                    if row[field].find(first_key) == 0:
                        result.append(row)
        else:
            if field and to_str(field) != '':
                for row in list_data:
                    if field in row:
                        if row[field].find(first_key) == 0:
                            result.append(row)
            else:
                for row in list_data:
                    if row:
                        v_index = row.find(first_key)
                        if v_index == 0:
                            result.append(row)
        return result

    def process_image_before_import(self, url, path):
        if not path:
            full_url = url
            path = strip_domain_from_url(url)
        else:
            full_url = join_url_path(url, path)
        if path and path.find('/wp-content/uploads/') != -1:
            newpath = path.split('/wp-content/uploads/')
            if newpath and to_len(newpath) > 1:
                path = newpath[1]
        path = re.sub(r"[^a-zA-Z0-9.-_()]", '', path)
        full_url = self.parse_url(full_url)
        return {
            'url': full_url,
            'path': path
        }

    def wpml_attributes_to_in_condition(self, list_keys):
        if not list_keys:
            return "('null')"
        result = "('tax_" + "','tax_".join([str(k) for k in list_keys]) + "')"
        return result

    def brand_image_in_condition(self, term_ids):
        if not term_ids:
            return "('null')"
        result = "('brand_taxonomy_image" + "','brand_taxonomy_image".join([str(k) for k in term_ids]) + "')"
        return result

    def detect_seo(self):
        return 'default_seo'

    def categories_default_seo(self, category, categories_ext):
        result = list()
        seo_cate = self.construct_seo_category()
        seo_cate['request_path'] = self._notice['src']['config']['product_category_base'].strip('/') + '/' + to_str(
            category['slug'])
        seo_cate['default'] = True
        result.append(seo_cate)
        return result

    def products_default_seo(self, product, products_ext):
        result = list()
        if self._notice['src']['config']['product_base'].find('%product_cat%') != -1:
            term_relationship = get_list_from_list_by_field(products_ext['data']['term_relationship'], 'object_id',
                                                            product['ID'])
            category_src = get_list_from_list_by_field(term_relationship, 'taxonomy', 'product_cat')
            if category_src:
                for product_category in category_src:
                    seo_product = self.construct_seo_product()
                    seo_product['request_path'] = self._notice['src']['config']['product_base'].strip(
                        '/') + '/' + to_str(product_category['slug']) + '/' + to_str(product['post_name'])
                    seo_product['category_id'] = product_category['term_id']
                    result.append(seo_product)
        else:
            seo_product = self.construct_seo_product()
            seo_product['request_path'] = self._notice['src']['config']['product_base'].strip('/') + '/' + to_str(
                product['post_name'])
            seo_product['default'] = True
            result.append(seo_product)
        if product['post_name']:
            seo_product = self.construct_seo_product()
            seo_product['request_path'] = to_str(product['post_name'])
            seo_product['default'] = True
            result.append(seo_product)
        return result

    def get_order_status_label(self, order_status):
        if not order_status:
            return ''
        order_status = order_status.replace('wc-', '')
        order_status = order_status.replace('-', ' ')
        order_status = order_status.capitalize()
        return order_status

    def get_woo_attribute_id(self, pro_attr_code, attribute_name, language_code=None, language_attribute_data=None,
                             attribute_type='select'):
        pro_attr_code = urllib.parse.unquote(pro_attr_code)
        woo_attribute_id = self.get_map_field_by_src(self.TYPE_ATTR, None, 'pa_' + pro_attr_code)
        if not woo_attribute_id:
            attribute_data = {
                'attribute_name': pro_attr_code,
                'attribute_type': attribute_type
            }
            attribute_result = self.select_data_connector(
                self.create_select_query_connector('woocommerce_attribute_taxonomies', attribute_data))
            woo_attribute_id = None
            if attribute_result and attribute_result['data']:
                woo_attribute_id = attribute_result['data'][0]['attribute_id']
            if not woo_attribute_id:
                pro_attr_data = {
                    'attribute_name': pro_attr_code,
                    'attribute_label': attribute_name,
                    'attribute_type': attribute_type,
                    'attribute_orderby': "menu_order",
                    'attribute_public': 0,
                }
                woo_attribute_id = self.import_data_connector(
                    self.create_insert_query_connector('woocommerce_attribute_taxonomies', pro_attr_data),
                    'products')
                if woo_attribute_id:
                    self.insert_map(self.TYPE_ATTR, None, woo_attribute_id, 'pa_' + pro_attr_code)

        if woo_attribute_id:
            if self.is_wpml():
                attribute_data_lang = self.get_convert_data_language(language_attribute_data, None, language_code,
                                                                     'option_languages')
                option_lang_name = attribute_data_lang.get('option_name')
                if not option_lang_name:
                    option_lang_name = attribute_data_lang.get('attribute_name')
                if option_lang_name != attribute_name:
                    translate_id = self.get_map_field_by_src('translate', woo_attribute_id, None, language_code)
                    if not translate_id:
                        translate_query = {
                            'icl_strings': self.create_select_query_connector('icl_strings',
                                                                              {'value': attribute_name,
                                                                               'name': 'taxonomy singular name: ' + attribute_name}),
                            'icl_string_translations': {
                                'type': 'select',
                                'query': "select * from _DBPRF_icl_string_translations where string_id in (" +
                                         self.create_select_query_connector('icl_strings', {'value': attribute_name,
                                                                                            'name': 'taxonomy singular name: ' + attribute_name},
                                                                            'id')['query'] + ")"
                            }
                        }
                        select = self.select_multiple_data_connector(translate_query)
                        if select['result'] == 'success':
                            icl_string_id = None
                            is_tranlate = False
                            if not select['data']['icl_strings']:
                                icl_strings_data = {
                                    'language': self._notice['target']['language_default'],
                                    'context': 'WordPress',
                                    'name': 'taxonomy singular name: ' + attribute_name,
                                    'value': attribute_name,
                                    'string_package_id': None,
                                    'wrap_tag': '',
                                    'type': 'LINE',
                                    'title': None,
                                    'status': 2,
                                    'gettext_context': '',
                                    'domain_name_context_md5': hashlib.md5(
                                        to_str('WordPresstaxonomy singular name: ' + attribute_name).encode()),
                                    'translation_priority': 'optional',
                                    'word_count': None
                                }
                                icl_string_id = self.import_product_data_connector(
                                    self.create_insert_query_connector('icl_strings', icl_strings_data))

                            else:
                                icl_string = select['data']['icl_strings'][0]
                                if icl_string['language'] != language_code:
                                    icl_string_id = icl_string['id']
                                    check = get_row_from_list_by_field(select['data']['icl_string_translations'],
                                                                       'language', language_code)
                                    is_tranlate = True if check else False
                                else:
                                    is_tranlate = True
                            if icl_string_id and not is_tranlate:
                                icl_string_translations_data = {
                                    'string_id': icl_string_id,
                                    'language': language_code,
                                    'status': 10,
                                    'value': option_lang_name,
                                    'translator_id': None,
                                    'translation_service': '',
                                    'batch_id': 0,
                                    'translation_date': get_current_time()
                                }
                                icl_string_translation_id = self.import_product_data_connector(
                                    self.create_insert_query_connector('icl_string_translations',
                                                                       icl_string_translations_data))
                                if icl_string_translation_id:
                                    self.insert_map('translate', woo_attribute_id, icl_string_translation_id, None,
                                                    None, None, language_code)
        return woo_attribute_id

    def get_woo_attribute_value(self, attribute_value, pro_attr_code, language_code=None, attribute_data=None,
                                desc=''):
        pro_attr_code = urllib.parse.unquote(pro_attr_code)
        if self.is_wpml():
            value_data = self.get_convert_data_language(attribute_data, None, language_code,
                                                        'option_value_languages')
            if value_data:
                attribute_value = value_data['option_value_name']
        attribute_value = to_str(attribute_value)[:200]
        slug_default = self.get_slug_attr(attribute_data)
        slug = self.get_slug_attr(attribute_data, language_code)
        opt_value_id = None
        # if opt_value_exist:
        # 	return opt_value_exist['id_desc']
        # opt_value_exist = self.select_map(self._migration_id, self.TYPE_ATTR_VALUE, None, None, 'pa_' + pro_attr_code, None, slug)
        opt_value_exist = self.select_map(self._migration_id, self.TYPE_ATTR_VALUE, None, None,
                                          'pa_' + pro_attr_code, None, slug, language_code)
        if opt_value_exist:
            if not self.is_wpml() or not language_code or language_code == self._notice['target'][
                'language_default']:
                return opt_value_exist['id_desc']
            else:
                opt_value_id = opt_value_exist['id_desc']
        if not opt_value_id:
            query = {
                'type': 'select',
                'query': 'SELECT * FROM _DBPRF_terms AS term LEFT JOIN _DBPRF_term_taxonomy AS taxonomy ON term.term_id = taxonomy.term_id WHERE term.name = ' + self.escape(
                    attribute_value) + " AND taxonomy.taxonomy = " + self.escape('pa_' + pro_attr_code)
            }
            attribute_result = self.select_data_connector(query)
            if attribute_result and attribute_result['data']:
                opt_value_id = attribute_result['data'][0]['term_taxonomy_id']
            if not opt_value_id:
                if self.is_wpml() and language_code != self._notice['target']['language_default']:
                    new_slug = slug_default + '-' + to_str(language_code) if slug == slug_default else slug
                else:
                    new_slug = slug_default
                value_term = {
                    'name': attribute_value,
                    'slug': new_slug,
                    'term_group': 0,
                }
                term_id = self.import_product_data_connector(
                    self.create_insert_query_connector('terms', value_term), 'products')
                value_term_taxonomy = {
                    'term_id': term_id,
                    'taxonomy': 'pa_' + pro_attr_code,
                    'description': desc,
                    'parent': 0,
                    'count': 0
                }
                opt_value_id = self.import_product_data_connector(
                    self.create_insert_query_connector('term_taxonomy', value_term_taxonomy), 'products')
            if opt_value_id:
                self.insert_map(self.TYPE_ATTR_VALUE, None, opt_value_id, 'pa_' + pro_attr_code, None, slug,
                                language_code)
        if opt_value_id:
            if self.is_wpml():
                attribute_data_lang = self.get_convert_data_language(attribute_data, None, language_code,
                                                                     'option_value_languages')
                if attribute_data_lang['option_value_name'] != attribute_value:
                    translate_query = {
                        'icl_translations': {
                            'type': 'select',
                            'query': 'select * from _DBPRF_icl_translations where trid in (select trid from wp_icl_translations where  ' + self.dict_to_where_condition(
                                {'element_id': opt_value_id, 'element_type': 'tax_pa_' + pro_attr_code}) + ')'
                        },
                        'term': {
                            'type': 'select',
                            'query': 'SELECT * FROM _DBPRF_terms AS term LEFT JOIN _DBPRF_term_taxonomy AS taxonomy ON term.term_id = taxonomy.term_id WHERE term.name = ' + self.escape(
                                attribute_data_lang[
                                    'option_value_name']) + " AND taxonomy.taxonomy = " + self.escape(
                                'pa_' + pro_attr_code)
                        }
                    }
                    select = self.select_multiple_data_connector(translate_query)
                    if select['result'] == 'success':
                        trid = None
                        is_tranlate = False
                        if not select['data']['icl_translations']:
                            trid = self.get_new_trid()
                            icl_translations_data = {
                                'language_code': self._notice['target']['language_default'],
                                'element_type': 'tax_pa_' + pro_attr_code,
                                'element_id': opt_value_id,
                                'trid': trid,
                                'source_language_code': None,
                            }
                            icl_translation_id = self.import_product_data_connector(
                                self.create_insert_query_connector('icl_translations', icl_translations_data))

                        else:
                            icl_translations = select['data']['icl_translations'][0]
                            trid = icl_translations['trid']
                            check = get_row_from_list_by_field(select['data']['icl_translations'], 'language_code',
                                                               language_code)
                            is_tranlate = True if check else False
                        if trid and not is_tranlate:
                            new_slug = slug_default + '-' + to_str(
                                language_code) if slug != slug_default else slug_default
                            value_term = {
                                'name': attribute_data_lang['option_value_name'],
                                'slug': new_slug,
                                'term_group': 0,
                            }
                            term_id = self.import_product_data_connector(
                                self.create_insert_query_connector('terms', value_term), 'products')
                            value_term_taxonomy = {
                                'term_id': term_id,
                                'taxonomy': 'pa_' + pro_attr_code,
                                'description': desc,
                                'parent': 0,
                                'count': 0
                            }
                            opt_value_id = self.import_product_data_connector(
                                self.create_insert_query_connector('term_taxonomy', value_term_taxonomy),
                                'products')
                            if opt_value_id:
                                icl_translations_data = {
                                    'language_code': language_code,
                                    'element_type': 'tax_pa_' + pro_attr_code,
                                    'element_id': opt_value_id,
                                    'trid': trid,
                                    'source_language_code': self._notice['target']['language_default'],
                                }
                                self.import_product_data_connector(
                                    self.create_insert_query_connector('icl_translations', icl_translations_data))
                                self.insert_map(self.TYPE_ATTR_VALUE, None, opt_value_id, 'pa_' + pro_attr_code,
                                                None, slug, language_code)

        return opt_value_id

    def to_timestamp(self, value, str_format='%Y-%m-%d %H:%M:%S'):
        try:
            timestamp = to_int(time.mktime(time.strptime(value, str_format)))
            if timestamp:
                return timestamp
            return to_int(time.time())
        except:
            return to_int(time.time())

    def get_map_field_by_src(self, map_type=None, id_src=None, code_src=None, lang=None, field='id_desc'):
        if not self.is_wpml() and not self.is_polylang() or map_type in [self.TYPE_PATH_IMAGE, self.TYPE_IMAGE]:
            return super().get_map_field_by_src(map_type, id_src, code_src, field)
        if not id_src and not code_src:
            return False
        _migration_id = self._migration_id
        # if id_src:
        # 	code_src = None
        # else:
        # 	code_src = None
        map_data = self.select_map(_migration_id, map_type, id_src, None, code_src, None, None, lang)
        if not map_data:
            return False
        return map_data.get(field, False)

    def select_map(self, _migration_id=None, map_type=None, id_src=None, id_desc=None, code_src=None,
                   code_desc=None, value=None, lang=None):
        if not self.is_wpml() and not self.is_polylang() or map_type in [self.TYPE_PATH_IMAGE, self.TYPE_IMAGE]:
            return super().select_map(_migration_id, map_type, id_src, id_desc, code_src, code_desc, value)
        where = dict()
        if _migration_id:
            where['migration_id'] = _migration_id
        if map_type:
            where['type'] = map_type
        if id_src:
            where['id_src'] = id_src
        if id_desc:
            where['id_desc'] = id_desc
        if code_src:
            where['code_src'] = code_src
        if code_desc:
            where['code_desc'] = code_desc
        if value:
            where['value'] = value
        if (self.is_wpml() or self.is_polylang()) and map_type in [self.TYPE_CATEGORY, self.TYPE_PRODUCT,
                                                                   self.TYPE_ATTR, self.TYPE_ATTR_VALUE]:
            where['lang'] = lang
        if not where:
            return None
        result = self.select_obj(TABLE_MAP, where)
        try:
            data = result['data'][0]
        except Exception as e:
            data = None
        return data

    def insert_map(self, map_type=None, id_src=None, id_desc=None, code_src=None, code_desc=None, value=None,
                   lang=None):
        if to_int(id_src) == 0 and to_str(id_src) != '0':
            id_src = None
        data_inset = {
            'migration_id': self._migration_id,
            'type': map_type,
            'id_src': id_src,
            'code_src': code_src,
            'id_desc': id_desc,
            'code_desc': code_desc,
            'value': value,
        }
        if self.is_wpml() or self.is_polylang():
            data_inset['lang'] = lang
        insert = self.insert_obj(TABLE_MAP, data_inset)
        if (not insert) or (insert['result'] != 'success'):
            return False
        return insert['data']

    def is_wpml(self):
        return self._notice[self.get_type()]['support'].get('wpml')

    def is_polylang(self):
        return self._notice[self.get_type()]['support'].get('polylang')

    def get_convert_data_language(self, convert, src_language_id=None, target_language_id=None,
                                  key_language='languages'):
        if not self.is_wpml() and not self.is_polylang():
            return convert
        list_language_data = convert.get(key_language)
        if not list_language_data:
            return convert
        language_data = None
        if src_language_id:
            if list_language_data.get(to_str(src_language_id)):
                language_data = list_language_data[to_str(src_language_id)]
        elif target_language_id:
            for src_id, data in list_language_data.items():
                if self._notice['map']['languages'].get(to_str(src_id)) == target_language_id:
                    language_data = data
                    break
        if not language_data:
            return convert
        for key_lang, value in language_data.items():
            if not value:
                continue
            if key_lang == 'option_value_name' and convert.get(
                    'option_type') == self.OPTION_MULTISELECT and 'position_option' in convert:
                value_lang = to_str(value).split(';')
                if len(value_lang) > to_int(convert.get('position_option')):
                    value = value_lang[to_int(convert.get('position_option'))]
            convert[key_lang] = value
        return convert

    def get_pro_attr_code_default(self, option):
        if self.is_wpml():
            option = self.get_convert_data_language(option, None, self._notice['target']['language_default'],
                                                    'option_languages')
        pro_attr_code = to_str(option['option_name']).lower()
        # attribute_name = option['option_name']
        pro_attr_code = pro_attr_code.replace(' ', '_')
        if option['option_code']:
            pro_attr_code = to_str(option['option_code']).lower()
            pro_attr_code = pro_attr_code.replace(' ', '_')
        pro_attr_code_len = 28
        check_encode = chardet.detect(pro_attr_code.encode())
        if check_encode['encoding'] != 'ascii':
            pro_attr_code = pro_attr_code[0:14]
            pro_attr_code_len = 200
        pro_attr_code = self.sanitize_title(pro_attr_code, pro_attr_code_len)
        return pro_attr_code

    def get_slug_attr(self, option_value, language_code=None):
        if option_value['option_value_code']:
            return self.sanitize_title(to_str(option_value['option_value_code'])).lower()
        attribute_value = option_value['option_value_name']
        if self.is_wpml():
            if not language_code:
                language_code = self._notice['target']['language_default']
            value_data = self.get_convert_data_language(option_value, None, language_code, 'option_value_languages')
            if value_data:
                attribute_value = value_data['option_value_name']
        return self.sanitize_title(to_str(attribute_value).lower())

    def get_key_check_default(self, attributes):
        key_check = ''
        for children_attribute in attributes:
            if self.is_wpml():
                children_attribute = self.get_convert_data_language(children_attribute, None,
                                                                    self._notice['target']['language_default'],
                                                                    'option_value_languages')
            if key_check:
                key_check += '|'
            key_check += to_str(children_attribute['option_name']) + ':' + to_str(
                children_attribute['option_value_name'])
        return key_check

    def lecm_rewrite_table_construct(self):
        return {
            'table': '_DBPRF_lecm_rewrite',
            'rows': {
                'id': 'INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY',
                'link': 'VARCHAR(255)',
                'type': 'VARCHAR(255)',
                'type_id': 'INT(11)',
                'redirect_type': 'SMALLINT(5)',
            },
        }

    def is_woo2woo(self):
        return self._notice['src']['cart_type'] == self._notice['target']['cart_type']

    def check_sync_child(self, child, combination, check_any=False):
        for attribute in combination:
            if not check_any:
                if to_str(child.get(attribute['option_name'])) != to_str(attribute['option_value_name']):
                    if to_str(child.get(to_str(attribute['option_code']).replace(' ', '-'))) != to_str(
                            attribute['option_value_name']):
                        return False
            elif to_str(child.get(attribute['option_name'])) and to_str(
                    child.get(to_str(attribute['option_code']).replace(' ', '-'))) != to_str(
                attribute['option_value_name']):
                return False
        return True

    def select_all_category_map(self):
        where = dict()
        where['migration_id'] = self._migration_id
        where['type'] = self.TYPE_CATEGORY if not self.blog_running else self.TYPE_CATEGORY_BLOG
        result = self.select_obj(TABLE_MAP, where)
        data = list()
        if result['result'] == 'success' and result['data']:
            data = result['data']

        result_data = list()
        if data:
            for row in data:
                value = row['id_desc']
                result_data.append(value)

        return result_data

    def create_file_variant_limit(self):
        file_path = get_pub_path() + '/media/' + to_str(self._migration_id)
        if not os.path.exists(file_path):
            os.makedirs(file_path, mode=0o777)
        file_name = file_path + '/variants.csv'
        column = ['src_id', 'target_id', 'name', 'sku', 'variants']
        with open(file_name, mode='a') as employee_file:
            employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow(column)
        return

    def warning_variant_limit(self, convert):
        if convert['id']:
            product = "#" + to_str(convert['id'])
        else:
            product = ': ' + to_str(convert['code'])
        self.sleep_time(0, 'variant', True, msg=product)

    def log_variant_limit(self, product_id, convert, variants):
        self.is_variant_limit = True
        file_name = get_pub_path() + '/media/' + to_str(self._migration_id) + '/variants.csv'
        if not os.path.isfile(file_name):
            self.create_file_variant_limit()
        column = [convert['id'] if convert['id'] else convert['code'], product_id, convert['name'], convert['sku'],
                  variants]

        with open(file_name, mode='a') as employee_file:
            employee_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            employee_writer.writerow(column)
        return

    def check_slug_exist(self, slug=None):
        select = {
            'slug': slug,
        }
        category_data = self.select_data_connector(self.create_select_query_connector('terms', select))

        try:
            term_id = category_data['data'][0]['term_id']
        except Exception:
            term_id = False
        return term_id

    def get_query_img_wpml(self, img_id, language_code):
        source_language_code = self._notice['target']['language_default']
        default_language_code = language_code
        if source_language_code == default_language_code:
            default_language_code = source_language_code
            source_language_code = None
        trid = self.get_new_trid()
        wpml_img_data = {
            'element_type': 'post_attachment',
            'element_id': img_id,
            'trid': trid,
            'language_code': default_language_code,
            'source_language_code': source_language_code
        }
        wpml_img_query = self.create_insert_query_connector("icl_translations", wpml_img_data)
        return wpml_img_query

    def check_exist_code_product(self, code_product):
        check = self.select_data_connector(self.create_select_query_connector('posts', {'posttype'}))

    def _get_customer_lookup_id(self, user_id):
        if not user_id:
            return 0
        select = {
            'user_id': user_id,
        }
        customer_lookup_data = self.select_data_connector(
            self.create_select_query_connector('wc_customer_lookup', select))

        try:
            customer_lookup_id = customer_lookup_data['data'][0]['customer_id']
        except Exception:
            customer_lookup_id = 0
        return customer_lookup_id
