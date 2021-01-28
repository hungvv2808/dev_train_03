import html
import math

from v32.cartmigration.libs.utils import *
from v32.cartmigration.libs.utils import to_str, response_error, response_success
from v32.cartmigration.models.basecart import LeBasecart


class LeCartCustom(LeBasecart):
    # url for shopify
    URL_SHOPIFY = 'http://www.iemotorsport.com/mm5/'
    # url for wq
    URL_WP = 'http://localhost/customcart/image/'

    def display_config_source(self):
        parent = super().display_config_source()
        if parent['result'] != 'success':
            return parent
        response = response_success()
        order_status_data = {
            'completed': 'Completed'
        }
        language_data = {
            1: "Default Language"
        }
        self._notice['src']['category_root'] = 1
        self._notice['src']['site'] = {
            1: 'Default Shop'
        }
        self._notice['src']['category_data'] = {
            1: 'Default Category',
        }
        self._notice['src']['support']['language_map'] = True
        self._notice['src']['support']['country_map'] = False
        self._notice['src']['support']['customer_group_map'] = False
        self._notice['src']['support']['taxes'] = True
        self._notice['src']['support']['manufacturers'] = False
        self._notice['src']['support']['reviews'] = False
        self._notice['src']['support']['add_new'] = True
        self._notice['src']['support']['skip_demo'] = False
        self._notice['src']['support']['customer_group_map'] = False
        self._notice['src']['languages'] = language_data
        self._notice['src']['order_status'] = order_status_data
        response['result'] = 'success'
        return response

    def display_config_target(self):
        return response_success()

    def display_import_source(self):
        if self._notice['config']['add_new']:
            recent = self.get_recent(self._migration_id)
            if recent:
                types = ['taxes', 'manufacturers', 'categories', 'attributes', 'products', 'customers', 'orders',
                         'reviews', 'pages', 'blogs', 'coupons', 'cartrules']
                for _type in types:
                    self._notice['process'][_type]['id_src'] = recent['process'][_type]['id_src']
                    self._notice['process'][_type]['total'] = 0
                    self._notice['process'][_type]['imported'] = 0
                    self._notice['process'][_type]['error'] = 0
        queries = {
            'categories': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_categories WHERE categories_id > " + to_str(
                    self._notice['process']['categories']['id_src']),
            },
            'products': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_products WHERE products_id > " + to_str(
                    self._notice['process']['products']['id_src']),
            },
            'customers': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_customers WHERE customers_id > " + to_str(
                    self._notice['process']['customers']['id_src']),
            },
            'orders': {
                'type': 'select',
                'query': "SELECT COUNT(1) AS count FROM _DBPRF_orders WHERE orders_id > " + to_str(
                    self._notice['process']['orders']['id_src']),
            },
        }
        count = self.select_multiple_data_connector(queries)
        if (not count) or (count['result'] != 'success'):
            return response_error()
        real_totals = dict()
        for key, row in count['data'].items():
            total = self.list_to_count_import(row, 'count')
            real_totals[key] = total
        for key, total in real_totals.items():
            self._notice['process'][key]['total'] = total
        return response_success()

    def display_import_target(self):
        return response_success()

    def display_confirm_source(self):
        return response_success()

    def display_confirm_target(self):
        self._notice['target']['clear']['function'] = 'clear_target_taxes'
        return response_success()

    # TODO: CLEAR

    def clear_target_categories(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_products',
        }

        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    def clear_target_products(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_customers',
        }

        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    def clear_target_customers(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_orders',
        }

        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    def clear_target_orders(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_reviews',
        }

        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    # TODO: CATEGORY
    def prepare_categories_import(self):
        return self

    def prepare_categories_export(self):
        return self

    def get_categories_main_export(self):
        id_src = self._notice['process']['categories']['id_src']
        limit = self._notice['setting']['categories']
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_categories WHERE categories_id > " + to_str(
                id_src) + " ORDER BY categories_id ASC LIMIT " + to_str(limit)
        }
        categories = self.select_data_connector(query)
        if not categories or categories['result'] != 'success':
            return response_error('could not get manufacturers main to export')
        return categories

    def get_categories_ext_export(self, categories):
        url_query = self.get_connector_url('query')
        category_ids = duplicate_field_value_from_list(categories['data'], 'categories_id')
        categories_ext_queries = {
            'categories_description': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_categories c INNER JOIN "
                         + " (SELECT * "
                         + " FROM _DBPRF_categories_description cd "
                         + " INNER JOIN _DBPRF_languages l on cd.language_id = l.languages_id) tmp "
                         + " ON tmp.categories_id = c.categories_id "
                         + " WHERE c.categories_id IN "
                         + self.list_to_in_condition(category_ids)
            },
            'products_to_categories': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_categories c INNER JOIN _DBPRF_products_to_categories p ON c.categories_id = p.categories_id WHERE c.categories_id IN "
                         + self.list_to_in_condition(category_ids)
            },
        }
        categories_ext = self.select_multiple_data_connector(categories_ext_queries)

        if not categories_ext or categories_ext['result'] != 'success':
            return response_warning()
        return categories_ext

    def convert_category_export(self, category, categories_ext):
        category_data = self.construct_category()
        parent = self.construct_category_parent()
        parent['id'] = 0
        if category['parent_id']:
            parent_data = self.get_category_parent(category['parent_id'])
            if parent_data['result'] == 'success':
                parent = parent_data['data']

        # set data from get to base form
        category_data['id'] = category['categories_id']
        category_data['parent'] = parent
        img = category['categories_image']
        if img and img != '':
            category_data['thumb_image']['label'] = img
            category_data['thumb_image']['url'] = self.URL_WP
            category_data['thumb_image']['path'] = img

        categories_description = get_row_from_list_by_field(categories_ext['data']['categories_description'],
                                                            'categories_id', category['categories_id'])
        category_data['name'] = categories_description['categories_name']
        category_data['description'] = categories_description['categories_name']
        category_data['short_description'] = categories_description['categories_name']
        category_data['sort_order'] = category['sort_order']
        category_data['created_at'] = category['date_added'] if category['date_added'] else get_current_time()
        category_data['updated_at'] = category['last_modified'] if category['last_modified'] else get_current_time()

        for language in self._notice['src']['languages_select']:
            cl = self.construct_category_lang()
            cl['name'] = category_data['name']
        category_data['languages'][categories_description['language_id']] = cl

        # meta_description
        category_data['category'] = category
        category_data['categories_ext'] = categories_ext

        if self._notice['config']['seo_301']:
            detect_seo = self.detect_seo()
            category_data['seo'] = getattr(self, 'categories_' + detect_seo)(category, categories_ext)

        return response_success(category_data)

    def get_category_id_import(self, convert, category, categories_ext):
        return category['categories_id']

    def check_category_import(self, convert, category, categories_ext):
        id_imported = self.get_map_field_by_src(self.TYPE_CATEGORY, convert['id'], convert['code'])
        return id_imported

    def router_category_import(self, convert, category, categories_ext):
        return response_success('category_import')

    def before_category_import(self, convert, category, categories_ext):
        return response_success()

    def category_import(self, convert, category, categories_ext):
        return response_success(0)

    def after_category_import(self, category_id, convert, category, categories_ext):
        return response_success()

    def addition_category_import(self, convert, category, categories_ext):
        return response_success()

    # TODO: PRODUCT
    def prepare_products_import(self):
        return self

    def prepare_products_export(self):
        return self

    def get_products_main_export(self):
        id_src = self._notice['process']['products']['id_src']
        limit = 4
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_products WHERE products_id > " + to_str(id_src)
                     + " ORDER BY products_id ASC LIMIT " + to_str(limit)
        }
        products = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        if not products or products['result'] != 'success':
            return response_error()
        return products

    def get_products_ext_export(self, products):
        url_query = self.get_connector_url('query')
        product_ids = duplicate_field_value_from_list(products['data'], 'products_id')
        product_id_con = self.list_to_in_condition(product_ids)
        product_ext_queries = {
            'products_description': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_products p INNER JOIN "
                         + " (SELECT * "
                         + "FROM _DBPRF_products_description pd INNER JOIN _DBPRF_languages l "
                         + "ON pd.language_id = l.languages_id) tmp ON tmp.products_id = p.products_id "
                         + " WHERE p.products_id IN "
                         + product_id_con,
            },
            'products_to_categories': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_products p INNER JOIN _DBPRF_products_to_categories ptc "
                         + " ON p.products_id = ptc.products_id WHERE p.products_id IN "
                         + product_id_con,
            },
            'attributes': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_attribute ",
            },
            'product_attribute': {
                'type': 'select',
                'query': "SELECT pa.* FROM _DBPRF_products p "
                         + " INNER JOIN _DBPRF_product_attribute pa ON p.products_id = pa.product_id "
                         + " WHERE p.products_id IN "
                         + product_id_con,
            },
        }
        product_ext = self.get_connector_data(url_query, {
            'serialize': True, 'query': json.dumps(product_ext_queries)
        })
        if (not product_ext) or product_ext['result'] != 'success':
            return response_error()

        return product_ext

    def convert_product_export(self, product, products_ext):
        product_data = self.construct_product()

        product_description = get_row_from_list_by_field(
            products_ext['data']['products_description'],
            'products_id',
            product['products_id']
        )
        products_to_categories = get_row_from_list_by_field(
            products_ext['data']['products_to_categories'],
            'products_id',
            product['products_id']
        )

        product_data['id'] = product['products_id']

        if product['products_image']:
            product_data['thumb_image']['label'] = product['products_image']
            product_data['thumb_image']['url'] = self.URL_WP
            product_data['thumb_image']['path'] = product['products_image']

        product_data['name'] = product_description['products_name']
        product_data['sku'] = product['products_upc_code']
        product_data['barcode'] = product['products_upc_code']
        product_data['url_key'] = product_description['products_url']
        product_data['description'] = html.unescape(product_description['products_description'])
        product_data['short_description'] = html.unescape(product_description['products_description'])
        product_data['products_viewed'] = product_description['products_viewed']
        product_data['price'] = product['products_price']
        product_data['cost'] = product['products_cost']
        product_data['products_wholesale_price'] = product['products_wholesale_price']
        product_data['products_msrp'] = product['products_msrp']
        product_data['weight'] = product['products_weight']
        product_data['length'] = to_decimal(product['products_length'])
        product_data['width'] = to_decimal(product['products_width'])
        product_data['height'] = to_decimal(product['products_height'])
        product_data['status'] = True if to_int(product['products_status']) > 0 else False
        product_data['qty'] = product['products_quantity']
        product_data['manage_stock'] = True
        product_data['tax']['id'] = product['products_tax_class_id']
        product_data['manufacturer']['id'] = product['manufacturers_id']
        product_data['created_at'] = product['products_date_added'] if product[
            'products_date_added'] else get_current_time()
        product_data['updated_at'] = product['products_last_modified'] if product['products_last_modified'] else \
            product_data['created_at']
        product_data['date_available'] = product['products_date_available']

        if products_to_categories['categories_id']:
            for ptc in products_to_categories['categories_id']:
                ptc_d = self.construct_product_category()
                ptc_d['id'] = ptc
                product_data['categories'].append(ptc_d)

        attr = products_ext['data']['attributes']
        attributes = list()
        for a in attr:
            product_attribute = self.construct_product_attribute()
            product_attribute['option_id'] = a['attribute_id']
            product_attribute['option_name'] = a['attribute_name']
            product_attribute['option_code'] = a['attribute_code']
            product_attribute['option_type'] = a['attribute_type']
            attributes.append(product_attribute)

        product_data['attributes'] = attributes
        product_data['attributes_value'] = products_ext['data']['product_attribute']

        if self._notice['config']['seo_301']:
            detect_seo = self.detect_seo()
            product_data['seo'] = getattr(self, 'products_' + detect_seo)(product, products_ext)
        return response_success(product_data)

    def get_product_id_import(self, convert, product, products_ext):
        return product['products_id']

    def check_product_import(self, convert, product, products_ext):
        return True if self.get_map_field_by_src(self.TYPE_PRODUCT, convert['id'], convert['code']) else False

    def router_product_import(self, convert, product, products_ext):
        return response_success('product_import')

    def before_product_import(self, convert, product, products_ext):
        return response_success()

    def product_import(self, convert, product, products_ext):
        return response_success(0)

    def after_product_import(self, product_id, convert, product, products_ext):
        return response_success()

    def addition_product_import(self, convert, product, products_ext):
        return response_success()

    # TODO: CUSTOMER
    def prepare_customers_import(self):
        query = {
            'type': 'query',
            'query': "ALTER TABLE _DBPRF_customers MODIFY COLUMN customers_password varchar(255)"
        }
        self.import_data_connector(query, 'customer')
        return self

    def prepare_customers_export(self):
        return self

    def get_customers_main_export(self):
        id_src = self._notice['process']['customers']['id_src']
        limit = self._notice['setting']['customers']
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_customers WHERE customers_id > "
                     + to_str(id_src) + " ORDER BY customers_id ASC LIMIT " + to_str(limit)
        }

        customers = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        if not customers or customers['result'] != 'success':
            return response_error('could not get customers main to export')
        return customers

    def get_customers_ext_export(self, customers):
        url_query = self.get_connector_url('query')
        customers_ids = duplicate_field_value_from_list(customers['data'], 'customers_id')
        customer_ext_queries = {
            'address_book': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_customers c INNER JOIN "
                         + "(SELECT * FROM _DBPRF_address_book a "
                         + " LEFT JOIN _DBPRF_countries ct ON a.entry_country_id = ct.countries_id "
                         + " LEFT JOIN _DBPRF_zones z ON a.entry_zone_id = z.zone_id) tmp "
                         + " ON tmp.customers_id = c.customers_id "
                         + " WHERE c.customers_id IN "
                         + self.list_to_in_condition(customers_ids),
            },
            'customers_info': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_customers c INNER JOIN "
                         + " _DBPRF_customers_info ci ON c.customers_id = ci.customers_info_id "
                         + " WHERE c.customers_id IN "
                         + self.list_to_in_condition(customers_ids),
            },
        }
        customers_ext = self.get_connector_data(url_query,
                                                {'serialize': True, 'query': json.dumps(customer_ext_queries)})
        if not customers_ext or customers_ext['result'] != 'success':
            return response_error()
        return customers_ext

    def convert_customer_export(self, customer, customers_ext):
        customer_data = self.construct_customer()

        customers_info = get_row_from_list_by_field(
            customers_ext['data']['customers_info'],
            'customers_id',
            customer['customers_id']
        )
        address_book = get_list_from_list_by_field(
            customers_ext['data']['address_book'],
            'customers_id',
            customer['customers_id']
        )

        customer_data['phone'] = customer['customers_telephone']
        customer_data['id'] = customer['customers_id']
        customer_data['code'] = 'US'
        customer_data['username'] = customer['customers_email_address']
        customer_data['email'] = customer['customers_email_address']
        customer_data['password'] = customer['customers_password']
        customer_data['first_name'] = customer['customers_firstname']
        customer_data['last_name'] = customer['customers_lastname']
        customer_data['gender'] = customer['customers_gender']
        customer_data['dob'] = customer['customers_dob']
        customer_data['created_at'] = customers_info['customers_info_date_account_created']
        customer_data['updated_at'] = customers_info['customers_info_date_account_last_modified']

        if address_book:
            for ab in address_book:
                ab_d = self.construct_customer_address()
                ab_d['id'] = ab['address_book_id']
                ab_d['code'] = ab['entry_postcode']
                ab_d['first_name'] = ab['entry_firstname']
                ab_d['last_name'] = ab['entry_lastname']
                ab_d['gender'] = ab['entry_gender']
                ab_d['address_1'] = ab['entry_street_address']
                ab_d['address_2'] = '%s/%s' % (ab['entry_city'], ab['entry_state'])
                ab_d['city'] = ab['entry_city']
                ab_d['country'] = {
                    'id': ab['countries_id'],
                    'code': '%s/%s' % (ab['countries_iso_code_2'], ab['countries_iso_code_3']),
                    'country_code': ab['countries_iso_code_2'],
                    'name': ab['countries_name'],
                }
                ab_d['state'] = {
                    'id': ab['zone_id'],
                    'code': ab['zone_country_id'],
                    'state_code': ab['zone_code'],
                    'name': ab['zone_name'],
                }
                ab_d['postcode'] = ab['entry_postcode']
                ab_d['telephone'] = customer['customers_telephone']
                ab_d['company'] = ab['entry_company']
                ab_d['fax'] = customer['customers_fax']
                ab_d['default'] = {
                    'billing': True,
                    'shipping': True,
                }
                ab_d['billing'] = True
                ab_d['shipping'] = True
                ab_d['created_at'] = customers_info['customers_info_date_account_created']
                ab_d['updated_at'] = customers_info['customers_info_date_account_last_modified']
                if customer['customers_id'] == ab['customers_id']:
                    customer_data['address'].append(ab_d)
                else:
                    customer_data['address'].insert(0, ab_d)

        return response_success(customer_data)

    def get_customer_id_import(self, convert, customer, customers_ext):
        return customer['customers_id']

    def check_customer_import(self, convert, customer, customers_ext):
        return True if self.get_map_field_by_src(self.TYPE_CUSTOMER, convert['id'], convert['code']) else False

    def router_customer_import(self, convert, customer, customers_ext):
        return response_success('customer_import')

    def before_customer_import(self, convert, customer, customers_ext):
        return response_success()

    def customer_import(self, convert, customer, customers_ext):
        return response_success(0)

    def after_customer_import(self, customer_id, convert, customer, customers_ext):
        return response_success()

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
            'query': "SELECT * FROM _DBPRF_orders WHERE orders_id > "
                     + to_str(id_src) + " ORDER BY orders_id ASC LIMIT " + to_str(limit)
        }
        orders = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        if not orders or orders['result'] != 'success':
            return response_error('could not get orders main to export')
        return orders

    def get_orders_ext_export(self, orders):
        url_query = self.get_connector_url('query')
        order_ids = duplicate_field_value_from_list(orders['data'], 'orders_id')
        customer_ids = duplicate_field_value_from_list(orders['data'], 'client_customers_id')

        bil_country = duplicate_field_value_from_list(orders['data'], 'billing_country')
        delivery_country = duplicate_field_value_from_list(orders['data'], 'delivery_country')
        # countries - countries_name
        country_ids = '|'.join(set(bil_country + delivery_country))
        payment_zone = duplicate_field_value_from_list(orders['data'], 'billing_state')
        shipping_zone = duplicate_field_value_from_list(orders['data'], 'delivery_state')
        # zones - zone_code
        state_ids = '|'.join(set(payment_zone + shipping_zone))
        # print(f"ci: {country_ids}")
        # print(f"si: {state_ids}")

        orders_ext_queries = {
            'orders_products': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_orders_products op WHERE op.orders_id IN "
                         + self.list_to_in_condition(order_ids)
            },
            'orders_total': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_orders_total ot WHERE ot.orders_id IN "
                         + self.list_to_in_condition(order_ids)
            },
            'orders_customers': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_customers c "
                         + " LEFT JOIN _DBPRF_customers_info ci ON c.customers_id = ci.customers_info_id "
                         + " LEFT JOIN _DBPRF_address_book ab ON c.customers_id = ab.customers_id "
                         + " WHERE c.customers_id IN "
                         + self.list_to_in_condition(customer_ids)
            },
            'orders_countries': {
                'type': 'select',
                'query': f"SELECT * FROM _DBPRF_countries c WHERE c.countries_name RLIKE '{country_ids}'",
            },
            'orders_zones': {
                'type': 'select',
                'query': f"SELECT * FROM _DBPRF_zones z WHERE z.zone_code RLIKE '{state_ids}'",
            }
        }
        orders_ext = self.get_connector_data(url_query, {'serialize': True, 'query': json.dumps(orders_ext_queries)})
        if not orders_ext or orders_ext['result'] != 'success':
            return response_error()
        return orders_ext

    def convert_order_export(self, order, orders_ext):
        order_data = self.construct_order()

        # orders_products =

        orders_customers_products_total = get_list_from_list_by_field(
            orders_ext['data']['orders_customers_products_total'],
            'orders_id',
            order['orders_id']
        )

        order_data['id'] = order['orders_id']
        order_data['order_number'] = order['order_number']
        order_data['status'] = order['orders_status']

        order_items = list()
        for ocpt in orders_customers_products_total:
            order_data['code'] = ocpt['delivery_postcode'] if ocpt['delivery_postcode'] else ocpt['billing_postcode'],

            type_class = ocpt['class']
            if type_class == 'ot_tax':
                order_data['tax'] = self.set_type_order_total(ocpt)
            elif type_class == 'ot_discount':
                order_data['discount'] = self.set_type_order_total(ocpt)
            elif type_class == 'ot_shipping':
                order_data['shipping'] = self.set_type_order_total(ocpt)
            elif type_class == 'ot_subtotal':
                order_data['subtotal'] = self.set_type_order_total(ocpt)
            else:
                order_data['total'] = self.set_type_order_total(ocpt)

            order_data['currency'] = ocpt['currency']
            order_data['created_at'] = ocpt['date_purchased']

            orders_countries_c = orders_ext['data']['orders_countries'][0]
            orders_zones_c = orders_ext['data']['orders_zones'][0]
            tmp_address_c = self.set_tmp_address(ocpt, orders_countries_c, orders_zones_c, None)
            customer_address = {
                'gender': ocpt['entry_gender'],
                'default': {
                    'billing': True,
                    'shipping': True,
                },
                'billing': True,
                'shipping': True,
                'created_at': ocpt['customers_info_date_account_created'],
                'update_at': get_current_time(),
            }
            tmp_address_c.update(customer_address)

            order_data['customer'] = {
                'phone': ocpt['customers_telephone'],
                'id': ocpt['customers_id'],
                'code': ocpt['entry_postcode'],
                'note': '',
                'group_id': '',
                'username': ocpt['customers_email_address'],
                'email': ocpt['customers_email_address'],
                'password': ocpt['customers_password'],
                'first_name': ocpt['customers_firstname'],
                'middle_name': '',
                'last_name': ocpt['customers_lastname'],
                'gender': ocpt['customers_gender'],
                'dob': ocpt['customers_dob'],
                'is_subscribed': True,
                'active': True,
                'capabilities': list(),
                'created_at': ocpt['customers_info_date_account_created'],
                'update_at': get_current_time(),
                'address': tmp_address_c,
                'groups': list(),
                'balance': 0.00
            }

            order_data['customer_address'] = {
                'id': ocpt['address_book_id'],
                'code': ocpt['entry_postcode'],
                'username': ocpt['customers_email_address'],
                'email': ocpt['customers_email_address'],
                'first_name': ocpt['entry_firstname'],
                'middle_name': '',
                'last_name': ocpt['entry_lastname'],
            }

            # billing
            orders_countries_b = get_row_from_list_by_field(
                orders_ext['data']['orders_countries'],
                'countries_name',
                ocpt['billing_country']
            )
            orders_zones_b = get_row_from_list_by_field(
                orders_ext['data']['orders_zones'],
                'zone_code',
                ocpt['billing_state']
            )
            tmp_address_b = self.set_tmp_address(ocpt, orders_countries_b, orders_zones_b, 1)
            order_data['billing_address'] = tmp_address_b

            # delivery
            orders_countries_s = get_row_from_list_by_field(
                orders_ext['data']['orders_countries'],
                'countries_name',
                ocpt['delivery_country']
            )
            orders_zones_s = get_row_from_list_by_field(
                orders_ext['data']['orders_zones'],
                'zone_code',
                ocpt['delivery_state']
            )
            tmp_address_s = self.set_tmp_address(ocpt, orders_countries_s, orders_zones_s, 0)
            order_data['shipping_address'] = tmp_address_s

            order_data['payment'] = {
                'id': None,
                'code': None,
                'method': ocpt['payment_method'],
                'title': 'Payment Infor',
            }

            order_item_subtotal = to_decimal(ocpt['products_price']) * to_decimal(ocpt['products_quantity'])
            order_item_tax = to_decimal(ocpt['products_tax']) * to_decimal(ocpt['products_quantity'])
            order_item_total = to_decimal(order_item_subtotal) + to_decimal(order_item_tax)
            item = {
                'id': ocpt['orders_products_id'],
                'code': None,
                'product': {
                    'id': None,
                    'code': None,
                    'name': ocpt['products_name'],
                    'sku': '',
                },
                'qty': ocpt['products_quantity'],
                'price': ocpt['final_price'],
                'original_price': ocpt['products_price'],
                'tax_amount': order_item_tax,
                'tax_percent': (order_item_tax / order_item_subtotal) * 100 if order_item_subtotal != 0 else 0.0,
                'discount_amount': 0.0000,
                'discount_percent': 0.0000,
                'subtotal': order_item_subtotal,
                'total': order_item_total,
                'options': list(),
                'created_at': order['date_purchased'] if order['date_purchased'] else get_current_time(),
                'updated_at': get_current_time(),
            }
            if item not in order_items:
                order_items.append(item)
        order_data['items'] = order_items

        return response_success(order_data)

    def set_type_order_total(self, data):
        return {
            'title': data['title'],
            'amount': data['value'],
            'percent': data['products_tax'],
        }

    def set_tmp_address(self, data, country, state, type_d=None):
        if 'zone_id' not in state:
            pass
        tmp = {
            'id': data.get('address_book_id', None),
            'country': {
                'id': country.get('countries_id', None),
                'code': '%s/%s' % (country.get('countries_iso_code_2', 'VN'), country.get('countries_iso_code_3', 'vn')),
                'country_code': country.get('countries_iso_code_2', 'VN'),
                'name': country.get('countries_name', 'Vie'),
            },
            'state': {
                'id': state.get('zone_id', None),
                'code': state.get('zone_code', None),
                'state_code': state.get('zone_code', ''),
                'name': state.get('zone_name', ''),
            },
            'telephone': data['customers_telephone'],
            'fax': data['customers_fax'],
        }
        result = tmp

        if type_d is None:
            ext = {
                'code': data['entry_postcode'],
                'first_name': data['entry_firstname'],
                'middle_name': '',
                'last_name': data['entry_lastname'],
                'address_1': data['entry_street_address'],
                'address_2': '%s/%s' % (data['entry_city'], data['entry_state']),
                'city': data['entry_city'],
                'postcode': data['entry_postcode'],
                'company': data['entry_company'],
            }
            result.update(ext)
        elif type_d == 0:  # delivery
            ext = {
                'code': data['delivery_postcode'] if data['delivery_postcode'] else data['entry_postcode'],
                'first_name': data['delivery_name'].split(' ')[0] if data['delivery_name'] else data['entry_firstname'],
                'middle_name': '',
                'last_name': data['delivery_name'].split(' ')[1] if data['delivery_name'] else data['entry_firstname'],
                'address_1': data['delivery_address1'] if data['delivery_address1'] else data['entry_street_address'],
                'address_2': data['delivery_address2'] if data['delivery_address2'] else '%s/%s' % (data['entry_city'], data['entry_state']),
                'city': data['delivery_city'] if data['delivery_city'] else data['entry_city'],
                'postcode': data['delivery_postcode'] if data['delivery_postcode'] else data['entry_postcode'],
                'company': data['entry_company'] if data['entry_company'] else data['delivery_company'],
            }
            result.update(ext)
        else:  # billing
            ext = {
                'code': data['billing_postcode'] if data['billing_postcode'] else data['entry_postcode'],
                'first_name': data['billing_name'].split(' ')[0] if data['billing_name'] else data['entry_firstname'],
                'middle_name': '',
                'last_name': data['billing_name'].split(' ')[1] if data['billing_name'] else data['entry_firstname'],
                'address_1': data['billing_address1'] if data['billing_address1'] else data['entry_street_address'],
                'address_2': data['billing_address2'] if data['billing_address2'] else '%s/%s' % (data['entry_city'], data['entry_state']),
                'city': data['billing_city'] if data['billing_city'] else data['entry_city'],
                'postcode': data['billing_postcode'] if data['billing_postcode'] else data['entry_postcode'],
                'company': data['billing_country'] if data['billing_country'] else data['delivery_company'],
            }
            result.update(ext)
        return result

    def get_order_id_import(self, convert, order, orders_ext):
        return order['orders_id']

    def check_order_import(self, convert, order, orders_ext):
        return True if self.get_map_field_by_src(self.TYPE_ORDER, convert['id'], convert['code']) else False

    def router_order_import(self, convert, order, orders_ext):
        return response_success('order_import')

    def before_order_import(self, convert, order, orders_ext):
        return response_success()

    def order_import(self, convert, order, orders_ext):
        return response_success(0)

    def after_order_import(self, order_id, convert, order, orders_ext):
        return response_success()

    def addition_order_import(self, convert, order, orders_ext):
        return response_success()

    # todo: code opencart
    def _list_to_in_condition_product(self, products):
        if not products:
            return "('null')"
        products = list(map(self.escape, products))
        products = list(map(lambda x: to_str(x), products))
        res = "','product_id=".join(products)
        res = "('product_id=" + res + "')"
        return res

    def product_to_in_condition_seourl(self, ids):
        if not ids:
            return "('null')"
        result = "('product_id=" + "','product_id=".join([str(id) for id in ids]) + "')"
        return result

    def category_to_in_condition_seourl(self, ids):
        if not ids:
            return "('null')"
        result = "('category_id=" + "','category_id=".join([str(id) for id in ids]) + "')"
        return result

    def get_category_parent(self, category_id):
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_categories WHERE categories_id = " + to_str(category_id)
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

    def import_category_parent(self, convert_parent):
        parent_exists = self.get_map_field_by_src(self.TYPE_CATEGORY, convert_parent['id'], convert_parent['code'])
        if parent_exists:
            return response_success(parent_exists)
        category = convert_parent['category']
        categories_ext = convert_parent['categories_ext']
        category_parent_import = self.category_import(convert_parent, category, categories_ext)
        self.after_category_import(category_parent_import['data'], convert_parent, category, categories_ext)
        return category_parent_import

    def nl2br(self, string, is_xhtml=True):
        if is_xhtml:
            return string.replace('\n', '<br />\n')
        else:
            return string.replace('\n', '<br>\n')

    def get_country_id(self, code, name):
        query = 'SELECT countries_id FROM _DBPRF_countries '
        if code:
            query = query + ' WHERE countries_iso_code_2 = "' + to_str(code) + '"'
        elif name:
            query = query + ' WHERE countries_name = "' + to_str(name) + '"'
        countries_query = {
            'type': 'select',
            'query': query
        }
        countries = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(countries_query)})
        if not countries or countries['result'] != 'success' or not countries['data']:
            return 0
        return countries['data'][0]['country_id']

    def get_state_id(self, code, name):
        query = 'SELECT zone_id FROM _DBPRF_zones '
        if code:
            query = query + ' WHERE zone_code = "' + to_str(code) + '"'
        elif name:
            query = query + ' WHERE zone_name = "' + to_str(name) + '"'
        zones_query = {
            'type': 'select',
            'query': query
        }
        zones = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(zones_query)})
        if not zones or zones['result'] != 'success' or not zones['data']:
            return 0
        return zones['data'][0]['zone_id']

    def calculate_average_rating(self, rates, default='default'):
        rate = get_row_from_list_by_field(rates, 'rate_code', default)
        if rate and 'rate' in rate:
            return rate['rate']
        rate_total = 0
        count = to_len(rates)
        for _rate in rates:
            rate_total = rate_total + to_decimal(_rate['rate'])
        average = to_decimal(rate_total / count)
        if average > 5:
            return 5
        else:
            return math.ceil(average)

    def get_name_from_string(self, value):
        result = dict()
        parts = value.split(' ')
        result['lastname'] = parts.pop()
        result['firstname'] = " ".join(parts)
        return result

    def _cook_shipping_address_by_billing(self, shipping_address, billing_address):
        for key, value in shipping_address.items():
            if key in {'country', 'state'}:
                for child_key, child_value in shipping_address[key].items():
                    if not shipping_address[key][child_key]:
                        shipping_address[key][child_key] = billing_address[key][child_key]
            else:
                if not shipping_address[key]:
                    shipping_address[key] = billing_address[key]

        return shipping_address

    def convert_float_to_percent(self, value):
        return value * 100

    def get_con_store_select(self):
        select_store = self._notice['src']['languages_select'].copy()
        src_store = self._notice['src']['languages'].copy()
        if self._notice['src']['language_default'] not in select_store:
            select_store.append(self._notice['src']['language_default'])
        src_store_ids = list(src_store.keys())
        unselect_store = [item for item in src_store_ids if item not in select_store]
        select_store.append(0)
        if to_len(select_store) >= to_len(unselect_store):
            where = ' IN ' + self.list_to_in_condition(select_store) + ' '
        else:
            where = ' NOT IN ' + self.list_to_in_condition(unselect_store) + ' '

        return where

    def detect_seo(self):
        return 'default_seo'

    def categories_default_seo(self, category, categories_ext):
        result = list()
        type_seo = self.SEO_301
        category_url = get_list_from_list_by_field(categories_ext['data']['URIs'], 'cat_id', category['categories_id'])
        seo_cate = self.construct_seo_category()
        if category_url:
            for cate_url in category_url:
                seo_cate['request_path'] = cate_url['uri']
                seo_cate['default'] = True
                seo_cate['type'] = type_seo
                result.append(seo_cate)

        return result

    def products_default_seo(self, product, products_ext):
        result = list()
        type_seo = self.SEO_301
        category_url = get_list_from_list_by_field(products_ext['data']['URIs'], 'cat_id', product['products_id'])
        seo_cate = self.construct_seo_product()
        if category_url:
            for cate_url in category_url:
                seo_cate['request_path'] = cate_url['uri']
                seo_cate['default'] = True
                seo_cate['type'] = type_seo
                result.append(seo_cate)

        return result

    def to_url(self, name):
        new_name = re.sub(r"[^a-zA-Z0-9-. ]", '', name)
        new_name = new_name.replace(' ', '-')
        url = new_name.lower()
        return url
