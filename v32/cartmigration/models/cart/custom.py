import datetime
import html
import math

from v32.cartmigration.libs.utils import *
from v32.cartmigration.libs.utils import to_str, response_error, response_success
from v32.cartmigration.models.basecart import LeBasecart


class LeCartCustom(LeBasecart):
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

    def clear_target_taxes(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_manufacturers',
        }
        self._notice['target']['clear'] = next_clear
        return next_clear

    def clear_target_manufacturers(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_categories',
        }
        self._notice['target']['clear'] = next_clear
        return next_clear

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

    def clear_target_reviews(self):
        next_clear = {
            'result': 'process',
            'function': 'clear_target_pages',
        }

        self._notice['target']['clear'] = next_clear
        return self._notice['target']['clear']

    # TODO: TAX
    def prepare_taxes_import(self):
        return self

    def prepare_taxes_export(self):
        return self

    def get_taxes_main_export(self):
        id_src = self._notice['process']['taxes']['id_src']
        limit = self._notice['setting']['taxes']
        query = {
            'type': 'select',
            'query': "SELECT * FROM _DBPRF_StateTaxStates WHERE id > " + to_str(
                id_src) + " ORDER BY id ASC LIMIT " + to_str(limit)
        }
        taxes = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        if not taxes or taxes['result'] != 'success':
            return response_error('could not get taxes main to export')
        return taxes

    def get_taxes_ext_export(self, taxes):
        return response_success()

    def convert_tax_export(self, tax, taxes_ext):
        tax_product = list()
        tax_customer = list()
        tax_zone = list()
        tax_product_data = self.construct_tax_product()
        tax_product_data['id'] = 1
        tax_product_data['code'] = None
        tax_product_data['name'] = 'Product Tax Class Shopify'
        tax_product.append(tax_product_data)

        tax_zone_state = self.construct_tax_zone_state()

        tax_zone_country = self.construct_tax_zone_country()
        tax_zone_country['id'] = 'US'
        tax_zone_country['name'] = 'United States'
        tax_zone_country['country_code'] = 'US'

        tax_zone_rate = self.construct_tax_zone_rate()
        tax_zone_rate['id'] = None
        tax_zone_rate['name'] = tax['state'] + ' ' + tax['rate']
        tax_zone_rate['rate'] = tax['rate']

        tax_zone_data = self.construct_tax_zone()
        tax_zone_data['id'] = None
        tax_zone_data['name'] = 'United States'
        tax_zone_data['country'] = tax_zone_country
        tax_zone_state = self.construct_tax_zone_state()
        tax_zone_state['id'] = 'TX'
        tax_zone_state['name'] = 'Texas'
        tax_zone_state['state_code'] = 'TX'

        tax_zone_data['state'] = tax_zone_state
        tax_zone_data['rate'] = tax_zone_rate
        tax_zone.append(tax_zone_data)

        tax_data = self.construct_tax()
        tax_data['id'] = tax['id']
        tax_data['name'] = tax['state'] + ' ' + tax['rate']
        tax_data['tax_products'] = tax_product
        tax_data['tax_zones'] = tax_zone
        return response_success(tax_data)

    def get_tax_id_import(self, convert, tax, taxes_ext):
        return tax['id']

    def check_tax_import(self, convert, tax, taxes_ext):
        return True if self.get_map_field_by_src(self.TYPE_TAX, convert['id']) else False

    def router_tax_import(self, convert, tax, taxes_ext):
        return response_success('tax_import')

    def before_tax_import(self, convert, tax, taxes_ext):
        return response_success()

    def tax_import(self, convert, tax, taxes_ext):
        return response_success(0)

    def after_tax_import(self, tax_id, convert, tax, taxes_ext):
        return response_success()

    def addition_tax_import(self, convert, tax, taxes_ext):
        return response_success()

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
        img = get_row_from_list_by_field(
            category['categories_image'] if category['categories_image'] is not None else 'Demo',
            category['categories_image'], category['categories_image'])
        if img and img['image'] != '':
            category_data['thumb_image']['label'] = img['image']
            category_data['thumb_image']['url'] = 'https://unsplash.com/photos/KVyc-zNT8hE'
            category_data['thumb_image']['path'] = img['image']

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
        product_id_query = self.product_to_in_condition_seourl(product_ids)
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
        }
        product_ext = self.get_connector_data(url_query, {
            'serialize': True, 'query': json.dumps(product_ext_queries)
        })
        if (not product_ext) or product_ext['result'] != 'success':
            return response_error()

        return product_ext

    def convert_product_export(self, product, products_ext):
        products_ext_data = products_ext['data']
        product_data = self.construct_product()

        product_description = get_row_from_list_by_field(products_ext['data']['products_description'], 'products_id',
                                                         product['products_id'])
        products_to_categories = get_row_from_list_by_field(products_ext['data']['products_to_categories'],
                                                            'products_id',
                                                            product['products_id'])

        product_data['id'] = product['products_id']

        if product['products_image']:
            product_data['thumb_image']['label'] = product['products_image']
            product_data['thumb_image']['url'] = 'http://www.iemotorsport.com/mm5/'
            product_data['thumb_image']['path'] = product['products_image']

        product_data['name'] = product_description['products_name']
        product_data['sku'] = product['products_upc_code']
        product_data['barcode'] = product['products_upc_code']
        product_data['url_key'] = product_description['products_url']
        product_data['description'] = html.unescape(product_description['products_description'])
        product_data['short_description'] = html.unescape(product_description['products_description'])
        product_data['price'] = product['products_price']
        product_data['cost'] = product['products_cost']
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

        for language in self._notice['src']['languages_select']:
            pl = self.construct_product_lang()
            pl['name'] = product_data['name']
            pl['description'] = product_data['description']
            pl['short_description'] = product_data['description']
            pl['price'] = product_data['price']
        product_data['languages'][product_description['language_id']] = pl

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
            'query': "SELECT * FROM _DBPRF_customers WHERE customers_id > " + to_str(
                id_src) + " ORDER BY customers_id ASC LIMIT " + to_str(limit)
        }

        customers = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        if not customers or customers['result'] != 'success':
            return response_error('could not get customers main to export')
        return customers

    def get_customers_ext_export(self, customers):
        url_query = self.get_connector_url('query')
        customers_ids = duplicate_field_value_from_list(customers['data'], 'id')
        customer_ext_queries = {
            'address_book': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_customers c INNER JOIN _DBPRF_address_book a ON c.customers_id = a.customers_id WHERE customers_id IN " + self.list_to_in_condition(
                    customers_ids),
            },
        }
        customers_ext = self.get_connector_data(url_query,
                                                {'serialize': True, 'query': json.dumps(customer_ext_queries)})
        if not customers_ext or customers_ext['result'] != 'success':
            return response_error()
        country_ids = duplicate_field_value_from_list(customers_ext['data']['address_book'], 'cntry')
        zone_ids = duplicate_field_value_from_list(customers_ext['data']['address_book'], 'state')
        customers_ext_rel_queries = {
            'countries': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_countries WHERE countries_id IN  " + self.list_to_in_condition(
                    country_ids),
            },
        }
        customers_ext_rel = self.get_connector_data(url_query,
                                                    {'serialize': True, 'query': json.dumps(customers_ext_rel_queries)})
        if not customers_ext_rel or customers_ext_rel['result'] != 'success':
            return response_error()
        customers_ext = self.sync_connector_object(customers_ext, customers_ext_rel)
        return customers_ext

    def convert_customer_export(self, customer, customers_ext):
        # customer_data = self.construct_customer()
        customer_data = {
            'phone': '',
            'id': customer['id'],
            'code': None,
            'site_id': '',
            'group_id': '',
            'language_id': '',
            'username': customer['login'],
            'email': customer['pw_email'],
            'password': customer['password'],
            'first_name': customer['ship_fname'],
            'middle_name': '',
            'last_name': customer['ship_lname'],
            'gender': 'Male',
            'dob': '',
            'is_subscribed': False,
            'active': True,
            'capabilities': list(),
            'created_at': None,
            'updated_at': get_current_time(),
            'address': list(),
            'groups': list(),
            'balance': 0.00,
            'user_url': '',
            'telephone': customer['bill_phone'],
            'fax': customer['bill_fax']
        }
        # customer_data['group_id'] = customer['customer_group_id']
        # customer_data['dob'] = customer['customers_dob']
        # customer_data['is_subscribed'] = customer['customers_newsletter']

        # customer_info = get_row_from_list_by_field(customers_ext['data']['customers_info'], 'customers_info_id',
        # customer['customers_id']) if customer_info: customer_data['created_at'] = customer_info[
        # 'customers_info_date_account_created'] customer_data['updated_at'] = customer_info[
        # 'customers_info_date_account_last_modified'] if customer_data['dob'] == '0000-00-00 00:00:00' :
        # customer_data['dob']=customer_data['created_at']
        address_books = get_list_from_list_by_field(customers_ext['data']['address_book'], 'cust_id', customer['id'])
        if address_books:
            for address_book in address_books:
                address_data = self.construct_customer_address()
                address_data['id'] = address_book['id']
                address_data['first_name'] = address_book['fname']
                address_data['last_name'] = address_book['lname']
                # address_data['gender'] = address_book['entry_gender']
                address_data['address_1'] = get_value_by_key_in_dict(address_book, 'addr', '')
                address_data['address_2'] = get_value_by_key_in_dict(address_book, 'add2', '')
                address_data['city'] = address_book['city']
                address_data['postcode'] = address_book['zip']
                address_data['telephone'] = customer['bill_phone']
                address_data['company'] = address_book['comp']
                address_data['fax'] = customer['bill_fax']
                country = get_row_from_list_by_field(customers_ext['data']['countries'], 'alpha', address_book['cntry'])
                if country:
                    address_data['country']['id'] = country['id']
                    address_data['country']['country_code'] = country['alpha']
                    address_data['country']['name'] = country['name']
                else:
                    address_data['country']['id'] = address_book['cntry']
                state_id = address_book['state']
                if state_id:
                    state = get_row_from_list_by_field(customers_ext['data']['states'], 'code', state_id)
                    if state:
                        address_data['state']['id'] = state['code']
                        address_data['state']['state_code'] = state['code']
                        address_data['state']['name'] = state['name']
                    else:
                        address_data['state']['id'] = state_id
                else:
                    address_data['state']['name'] = 'AL'
                # if address_book['address_book_id'] == customer['customers_default_address_id']:
                address_data['default']['billing'] = True
                address_data['default']['shipping'] = True
                customer_data['address'].append(address_data)
        return response_success(customer_data)

    def get_customer_id_import(self, convert, customer, customers_ext):
        return customer['id']

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
            'query': "SELECT * FROM _DBPRF_orders WHERE orders_id > " + to_str(
                id_src) + " ORDER BY orders_id ASC LIMIT " + to_str(limit)
        }
        orders = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
        if not orders or orders['result'] != 'success':
            return response_error('could not get orders main to export')
        return orders

    def get_orders_ext_export(self, orders):
        url_query = self.get_connector_url('query')
        order_ids = duplicate_field_value_from_list(orders['data'], 'id')
        bil_country = duplicate_field_value_from_list(orders['data'], 'bill_cntry')
        delivery_country = duplicate_field_value_from_list(orders['data'], 'ship_cntry')
        country_ids = set(bil_country + delivery_country)

        payment_zone = duplicate_field_value_from_list(orders['data'], 'bill_state')
        shipping_zone = duplicate_field_value_from_list(orders['data'], 'ship_state')
        # cus_zone = duplicate_field_value_from_list(orders['data'], 'customers_state')
        state_ids = set(payment_zone + shipping_zone)
        cus_ids = duplicate_field_value_from_list(orders['data'], 'cust_id')
        orders_ext_queries = {
            'orders_products': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_orders o INNER JOIN _DBPRF_orders_products op ON o.orders_id = op.orders_id WHERE orders_id IN " + self.list_to_in_condition(
                    order_ids)
            },
            'orders_total': {
                'type': 'select',
                'query': "SELECT * FROM _DBPRF_orders o INNER JOIN _DBPRF_orders_total ot ON o.orders_id = ot.orders_id WHERE orders_id IN " + self.list_to_in_condition(
                    order_ids)
            },
        }
        orders_ext = self.get_connector_data(url_query, {'serialize': True, 'query': json.dumps(orders_ext_queries)})
        if not orders_ext or orders_ext['result'] != 'success':
            return response_error()
        return orders_ext

    def convert_order_export(self, order, orders_ext):
        order_data = self.construct_order()
        order_data['id'] = order['id']
        order_data['status'] = order['status']

        # order_total = get_list_from_list_by_field(orders_ext['data']['orders_total'], 'orders_id', order[
        # 	'orders_id'])
        # ot_tax = get_row_from_list_by_field(order_total, 'class', 'ot_tax')
        # ot_shipping = get_row_from_list_by_field(order_total, 'class', 'ot_shipping')
        # ot_subtotal = get_row_from_list_by_field(order_total, 'class', 'ot_subtotal')
        # ot_total = get_row_from_list_by_field(order_total, 'class', 'ot_total')
        # if ot_tax:
        # 	order_data['tax']['title'] = ot_tax['title']
        # 	order_data['tax']['amount'] = ot_tax['value']
        # 	if ot_subtotal:
        # 		order_data['tax']['percent'] = to_decimal(ot_tax['value']) / to_decimal(ot_subtotal['value'])
        order_data['tax']['title'] = 'Tax'
        order_data['tax']['amount'] = get_value_by_key_in_dict(order, 'total_tax', 0.0000)
        order_data['shipping']['title'] = 'Shipping'
        order_data['shipping']['amount'] = get_value_by_key_in_dict(order, 'total_ship', 0.0000)
        order_data['discount']['title'] = 'Discount'
        order_data['discount']['amount'] = 0.0000
        order_data['total']['title'] = 'Total'
        order_data['total']['amount'] = get_value_by_key_in_dict(order, 'total', 0.0000)
        order_data['subtotal']['title'] = 'Total products'
        order_data['subtotal']['amount'] = get_value_by_key_in_dict(order, 'total', 0.0000)
        order_data['currency'] = ''
        order_data['created_at'] = datetime.fromtimestamp(
            to_int(get_value_by_key_in_dict(order, 'orderdate', 0))).strftime('%Y-%m-%d %H:%M:%S')
        order_data['updated_at'] = get_current_time()

        # currency = get_row_value_from_list_by_field(orders_ext['data']['currencies'], 'code', order['currency'],
        # 'currencies_id') order_data['currency'] = order['currency'] order_data['currency_value'] = order[
        # 'currency_value'] order_data['created_at'] = order['date_purchased'] order_data['updated_at'] = order[
        # 'last_modified']

        order_customer = self.construct_order_customer()
        # order_customer = self.add_c(order_customer)
        order_customer['id'] = order['cust_id']
        order_customer['email'] = order['ship_email']
        customer_name = order['ship_fname'] + ' ' + order['ship_lname']
        order_customer['first_name'] = order['ship_fname']
        order_customer['last_name'] = order['ship_lname']
        order_data['customer'] = order_customer

        customer_address = self.construct_order_address()
        # customer_address = self.addConstructDefault(customer_address)
        customer_address['first_name'] = order['ship_fname']
        customer_address['last_name'] = order['ship_lname']
        customer_address['address_1'] = order['ship_addr']
        customer_address['address_2'] = order['ship_addr2']
        customer_address['city'] = order['ship_city']
        customer_address['postcode'] = order['ship_zip']
        customer_address['telephone'] = order['ship_phone']
        customer_address['company'] = order['ship_comp']

        customer_country = get_row_from_list_by_field(orders_ext['data']['countries'], 'alpha', order['bill_cntry'])
        if customer_country:
            customer_address['country']['id'] = customer_country['id']
            customer_address['country']['country_code'] = customer_country['alpha']
            customer_address['country']['name'] = customer_country['name']

        customer_state = get_row_from_list_by_field(orders_ext['data']['zones'], 'code', order['ship_state'])
        if customer_state:
            customer_address['state']['id'] = customer_state['code']
            customer_address['state']['state_code'] = customer_state['code']
            customer_address['state']['name'] = customer_state['name']

        order_data['customer_address'] = customer_address

        order_billing = self.construct_order_address()
        # order_billing = this->addConstructDefault(order_billing)
        billing_name = self.get_name_from_string(order['bill_fname'] + ' ' + order['bill_lname'])
        order_billing['first_name'] = order['bill_fname']
        order_billing['last_name'] = order['bill_lname']
        order_billing['address_1'] = order['bill_addr']
        order_billing['address_2'] = order['bill_addr2']
        order_billing['city'] = order['bill_city']
        order_billing['postcode'] = order['bill_zip']
        order_billing['telephone'] = order['bill_phone']
        order_billing['company'] = order['bill_comp']
        billing_country = get_row_from_list_by_field(orders_ext['data']['countries'], 'alpha', order['bill_cntry'])
        if billing_country:
            order_billing['country']['id'] = billing_country['id']
            order_billing['country']['code'] = billing_country['alpha']
            order_billing['country']['country_code'] = billing_country['alpha']
            order_billing['country']['name'] = billing_country['name']

        billing_state = get_row_from_list_by_field(orders_ext['data']['zones'], 'code', order[
            'bill_state'])
        if billing_state:
            order_billing['state']['id'] = billing_state['code']
            order_billing['state']['state_code'] = billing_state['code']
            order_billing['state']['name'] = billing_state['name']
        order_data['billing_address'] = order_billing

        order_delivery = self.construct_order_address()
        # order_delivery = self.addConstructDefault(order_delivery)
        delivery_name = self.get_name_from_string(order['ship_fname'] + ' ' + order['ship_lname'])
        order_delivery['first_name'] = order['ship_fname']
        order_delivery['last_name'] = order['ship_lname']
        order_delivery['address_1'] = order['ship_addr']
        order_delivery['address_2'] = order['ship_addr2']
        order_delivery['city'] = order['ship_city']
        order_delivery['postcode'] = order['ship_zip']
        order_delivery['telephone'] = order['ship_phone']
        order_delivery['company'] = order['ship_comp']

        delivery_country = get_row_from_list_by_field(orders_ext['data']['countries'], 'alpha', order['ship_cntry'])
        if delivery_country:
            order_delivery['country']['id'] = delivery_country['id']
            order_delivery['country']['code'] = delivery_country['alpha']
            order_delivery['country']['country_code'] = delivery_country['alpha']
            order_delivery['country']['name'] = delivery_country['name']
        delivery_state = get_row_from_list_by_field(orders_ext['data']['zones'], 'code', order['ship_state'])
        if delivery_state:
            order_delivery['state']['id'] = delivery_state['code']
            order_delivery['state']['state_code'] = delivery_state['code']
            order_delivery['state']['name'] = delivery_state['name']

        order_delivery = self._cook_shipping_address_by_billing(order_delivery, order_billing)
        order_data['shipping_address'] = order_delivery
        payments = get_row_from_list_by_field(orders_ext['data']['orders_payment'], 'order_id', order['id'])
        order_payment = self.construct_order_payment()
        order_payment['title'] = 'Payment'
        order_data['payment'] = order_payment

        order_products = get_list_from_list_by_field(orders_ext['data']['order_items'], 'order_id', order['id'])
        # order_product_attributes = get_list_from_list_by_field(orders_ext['data']['orders_products_attributes'],
        # 'orders_id', order['orders_id'])
        order_items = list()
        for order_product in order_products:
            order_item_subtotal = to_decimal(order_product['price']) * to_decimal(order_product['quantity'])
            order_item_tax = to_decimal(order_item_subtotal) * to_decimal(8.250) / 100
            order_item_total = to_decimal(order_item_subtotal) + to_decimal(order_item_tax)
            order_item = self.construct_order_item()
            # order_item = self.addConstructDefault(order_item)
            order_item['id'] = order_product['line_id']
            order_item['product']['id'] = order_product['product_id']
            order_item['product']['name'] = order_product['name']
            order_item['product']['sku'] = order_product['code']
            order_item['qty'] = order_product['quantity']
            order_item['price'] = order_product['price']
            order_item['original_price'] = order_product['base_price']
            # order_item['tax_amount'] = order_item_tax
            # order_item['tax_percent'] = order_product['products_tax']
            order_item['discount_amount'] = '0.0000'
            order_item['discount_percent'] = '0.0000'
            order_item['subtotal'] = order_item_subtotal
            order_item['total'] = order_item_subtotal
            # order_item_attributes = get_list_from_list_by_field(order_product_attributes, 'orders_products_id',
            # order_product['orders_products_id']) if order_item_attributes: order_item_options = list() for
            # order_product_attribute in order_item_attributes: order_item_option = self.construct_order_item_option()
            # order_item_option['option_name'] = order_product_attribute['products_options'] order_item_option[
            # 'option_value_name'] = order_product_attribute['products_options_values'] order_item_option['price'] =
            # order_product_attribute['options_values_price'] order_item_option['price_prefix'] =
            # order_product_attribute['price_prefix'] order_item_options.append(order_item_option)
            #
            # 	order_item['options'] = order_item_options

            order_items.append(order_item)
        order_data['items'] = order_items

        # order_status_history = get_list_from_list_by_field(orders_ext['data']['orders_status_history'],
        # 'orders_id', order['orders_id']) order_history = list() for orders_status_history in order_status_history:
        # order_history = self.construct_order_history() order_history['id'] = orders_status_history[
        # 'orders_status_history_id'] order_history['status'] = orders_status_history['orders_status_id']
        # order_history['comment'] = orders_status_history['comments'] order_history['notified'] =
        # orders_status_history['customer_notified'] order_history['created_at'] = orders_status_history['date_added']
        # order_data['history'].append(order_history)
        #
        return response_success(order_data)

    def get_order_id_import(self, convert, order, orders_ext):
        return order['id']

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

    # TODO: PAGE
    def prepare_pages_import(self):
        return self

    def prepare_pages_export(self):
        return self

    def get_pages_main_export(self):
        return response_success()

    def get_pages_ext_export(self, pages):
        return response_success()

    def convert_page_export(self, page, pages_ext):
        return response_success()

    def get_page_id_import(self, convert, page, pages_ext):
        return False

    def check_page_import(self, convert, page, pages_ext):
        return False

    def router_page_import(self, convert, page, pages_ext):
        return response_success('page_import')

    def before_page_import(self, convert, page, pages_ext):
        return response_success()

    def page_import(self, convert, page, pages_ext):
        return response_success(0)

    def after_page_import(self, page_id, convert, page, pages_ext):
        return response_success()

    def addition_page_import(self, convert, page, pages_ext):
        return response_success()

    # TODO: BLOCK
    def prepare_blogs_import(self):
        return response_success()

    def prepare_blogs_export(self):
        return self

    def get_blogs_main_export(self):
        return self

    def get_blogs_ext_export(self, blocks):
        return response_success()

    def convert_blog_export(self, block, blocks_ext):
        return response_success()

    def get_blog_id_import(self, convert, block, blocks_ext):
        return False

    def check_blog_import(self, convert, block, blocks_ext):
        return False

    def router_blog_import(self, convert, block, blocks_ext):
        return response_success('block_import')

    def before_blog_import(self, convert, block, blocks_ext):
        return response_success()

    def blog_import(self, convert, block, blocks_ext):
        return response_success(0)

    def after_blog_import(self, block_id, convert, block, blocks_ext):
        return response_success()

    def addition_blog_import(self, convert, block, blocks_ext):
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
