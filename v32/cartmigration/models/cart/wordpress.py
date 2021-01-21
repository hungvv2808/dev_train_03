import html
import unicodedata
from bs4 import BeautifulSoup

from urllib.request import Request, urlopen

import requests
from PIL import ImageFile
from cartmigration.models.basecart import LeBasecart
from cartmigration.libs.utils import *
import mimetypes

try:
	import chardet
except:
	pass

class LeCartWordpress(LeBasecart):
	WP_IMAGE = 'wp_image'
	def __init__(self, data = None):
		super().__init__(data)
		self.blog_running = False
		self.image_size = None
		self.exist_lecm_rewrite = None

	def display_config_source(self):
		parent = super().display_config_source()
		self._notice['src']['support']['blogs'] = True
		return response_success()

	def clear_target_pages(self):
		next_clear = {
			'result': 'success',
			'function': '',
			'msg': ''
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config']['pages']:
			return next_clear
		tables = [
			'postmeta',
			'posts'
		]
		non_delete = ['privacy-policy', 'shop', 'cart', 'checkout', 'my-account']
		for table in tables:
			where = ' post_type = "page" AND post_name NOT IN ' + self.list_to_in_condition(non_delete)
			if table == 'postmeta':
				where = ' post_id IN (SELECT ID FROM _DBPRF_posts WHERE  ' + where + ')'
			clear_table = self.get_connector_data(self.get_connector_url('query'), {
				'query': json.dumps({
					'type': 'query', 'query': "DELETE FROM `_DBPRF_" + table + "` WHERE " + where
				})
			})
			if (not clear_table) or (clear_table['result'] != 'success'):
				self.log("Clear data failed. Error: Could not empty table " + table, 'clear')
				continue
		return next_clear

	def clear_target_blogs(self):
		next_clear = {
			'result': 'process',
			'function': 'clear_target_coupons',
			'msg': ''
		}
		self._notice['target']['clear'] = next_clear
		if not self._notice['config'].get('blogs'):
			return next_clear
		all_queries = {
			'comment': {
				'type': 'delete',
				'query': 'DELETE FROM _DBPRF_comments WHERE comment_post_ID IN (SELECT ID FROM _DBPRF_posts WHERE post_type = "post")'
			},
			'term': {
				'type': 'delete',
				'query': 'DELETE FROM _DBPRF_terms WHERE term_id IN (SELECT term_id FROM _DBPRF_term_taxonomy WHERE taxonomy IN ' + self.list_to_in_condition(['category', 'post_tag']) + ')'
			},
			'term_taxonomy': {
				'type': 'delete',
				'query': 'DELETE FROM _DBPRF_term_taxonomy WHERE taxonomy IN ' + self.list_to_in_condition(['category', 'post_tag'])
			},
			'term_relationship': {
				'type': 'delete',
				'query': 'DELETE FROM _DBPRF_term_relationships WHERE object_id IN (SELECT ID FROM _DBPRF_posts WHERE post_type = "post")'
			},
			'postmeta': {
				'type': 'delete',
				'query': 'DELETE FROM _DBPRF_postmeta WHERE post_id IN (SELECT ID FROM _DBPRF_posts WHERE post_type = "post")'
			},
			'posts': {
				'type': 'delete',
				'query': 'DELETE FROM _DBPRF_posts WHERE post_type = "post"'
			},

		}
		delete = self.query_multiple_data_connector(all_queries, 'clear_blog')
		return next_clear

	# TODO: blog
	def prepare_blogs_import(self):
		self.blog_running = True
		return response_success()

	def prepare_blogs_export(self):
		self.blog_running = True
		return self

	def get_blogs_main_export(self):
		id_src = self._notice['process']['blogs']['id_src']
		limit = self._notice['setting']['blogs']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_posts WHERE ID > " + to_str(id_src) + "  AND post_type = 'post' ORDER BY ID ASC LIMIT " + to_str(limit),
		}
		blogs = self.select_data_connector(query, 'blogs')
		if not blogs or blogs['result'] != 'success':
			return response_error()
		return blogs

	def get_blogs_ext_export(self, blogs):
		blog_ids = duplicate_field_value_from_list(blogs['data'], 'ID')
		blog_id_con = self.list_to_in_condition(blog_ids)
		blog_ext_queries = {
			'postmeta': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_postmeta WHERE post_id IN " + blog_id_con
			},
			'term_relationship': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_term_relationships AS tr LEFT JOIN _DBPRF_term_taxonomy AS tx ON tx.term_taxonomy_id = tr.term_taxonomy_id LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id WHERE tr.object_id IN " + blog_id_con,
			},
			'comment_blog': {
				'type': "select",
				'query': "SELECT * FROM _DBPRF_comments WHERE comment_post_ID IN " + blog_id_con
			}
		}
		blogs_ext = self.select_multiple_data_connector(blog_ext_queries, 'blogs')
		if (not blogs_ext) or blogs_ext['result'] != 'success':
			return response_error()

		thumbnail_id_list = get_list_from_list_by_field(blogs_ext['data']['postmeta'], 'meta_key', '_thumbnail_id')
		thumbnail_ids = duplicate_field_value_from_list(thumbnail_id_list, 'meta_value')
		all_images_ids_query = self.list_to_in_condition(thumbnail_ids)
		blog_ext_rel_third_queries = {
			'image': {
				'type': 'select',
				'query': "SELECT p.ID, p.post_title, pm.meta_value, p.guid FROM _DBPRF_posts AS p LEFT JOIN _DBPRF_postmeta AS pm ON pm.post_id = p.ID AND pm.meta_key = '_wp_attached_file' WHERE p.ID IN " + all_images_ids_query,
			}
		}
		blogs_ext_third = self.select_multiple_data_connector(blog_ext_rel_third_queries, 'blogs')
		if (not blogs_ext_third) or blogs_ext_third['result'] != 'success':
			return response_error()

		blogs_ext = self.sync_connector_object(blogs_ext, blogs_ext_third)
		return blogs_ext

	def convert_blog_export(self, blog, blogs_ext):
		blog_data = self.construct_blog_post()
		blog_data['id'] = blog['ID']
		blog_data['title'] = blog['post_title'] if blog['post_title'] else (blog['post_name'] if blog['post_name'] else 'blog-' + to_str(blog['ID']))
		blog_data['url_key'] = blog['post_name']
		blog_data['status'] = True if blog['post_status'] == 'publish' else False
		blog_data['created_at'] = convert_format_time(blog['post_date'])
		blog_data['updated_at'] = convert_format_time(blog['post_modified'])
		blog_data['content'] = blog['post_content']
		blog_data['short_content'] = blog['post_excerpt']
		blog_data['comment_status'] = blog['comment_status']
		blog_meta = get_list_from_list_by_field(blogs_ext['data']['postmeta'], 'post_id', blog['ID'])
		blog_data['meta_description'] = self.get_value_metadata(blog_meta, '_yoast_wpseo_metadesc', '')
		blog_data['meta_title'] = self.get_value_metadata(blog_meta, '_yoast_wpseo_title', '')
		term_relationship = get_list_from_list_by_field(blogs_ext['data']['term_relationship'], 'object_id', blog['ID'])
		categories = get_list_from_list_by_field(term_relationship, 'taxonomy', 'category')
		if categories:
			categories_data = response_success(categories)
			categories_data['is_blog'] = True
			categories_ext = self.get_categories_ext_export(categories_data)
			for category_blog in categories:
				convert_category = self.convert_category_export(category_blog, categories_ext)
				if convert_category['result'] == 'success':
					blog_data['categories'].append(convert_category['data'])
		thumbnail_id = self.get_value_metadata(blog_meta, '_thumbnail_id', 0)
		if thumbnail_id:
			thumbnail_src = get_list_from_list_by_field(blogs_ext['data']['image'], 'ID', thumbnail_id)
			if thumbnail_src and thumbnail_src[0].get('meta_value'):
				blog_data['thumb_image']['label'] = thumbnail_src[0]['post_title']
				blog_data['thumb_image']['url'] = self._notice['src']['cart_url'].rstrip('/') + '/wp-content/uploads/' + to_str(thumbnail_src[0]['meta_value']).lstrip('/')
		blog_tags = get_list_from_list_by_field(term_relationship, 'taxonomy', 'post_tag')
		if blog_tags:
			tags = list()
			for blog_tag in blog_tags:
				tags.append(blog_tag['name'])
			if tags:
				blog_data['tags'] = ','.join(tags)
		blog_data['review'] = list()
		comment = get_list_from_list_by_field(blogs_ext['data']['comment_blog'], 'comment_post_ID', blog['ID'])
		if comment:
			for comment_post in comment:
				comment_data = self.construct_review()
				comment_data['id'] = comment_post['comment_ID']
				comment_data['parent_id'] = comment_post['comment_parent']
				comment_data['customer']['name'] = comment_post['comment_author']
				comment_data['customer']['code'] = comment_post['comment_author_email']
				comment_data['customer']['id'] = comment_post['user_id']
				comment_data['content'] = comment_post['comment_content']
				rv_status = {
					'0': 2,  # pending
					'1': 1,  # approved
					'spam': 3  # not approved
				}
				comment_data['status'] = rv_status.get(to_str(comment_post['comment_approved']), 'spam')
				comment_data['created_at'] = convert_format_time(comment_post['comment_date'])
				comment_data['updated_at'] = convert_format_time(comment_post['comment_date_gmt'])
				blog_data['review'].append(comment_data)
		return response_success(blog_data)

	def get_blog_id_import(self, convert, blog, blogs_ext):
		return blog['ID']

	def check_blog_import(self, convert, blog, blogs_ext):
		return True if self.get_map_field_by_src(self.TYPE_BLOG, convert['id'], convert['code']) else False

	def router_blog_import(self, convert, blog, blogs_ext):
		return response_success('blog_import')

	def before_blog_import(self, convert, blog, blogs_ext):
		return response_success()

	def blog_import(self, convert, blog, blogs_ext):
		blog_data = {
			'post_author': 1,
			'post_date': convert['created_at'] if convert['created_at'] else get_current_time(),
			'post_date_gmt': convert['created_at'] if convert['created_at'] else get_current_time(),
			'post_content': self.replace_url_src_to_url_target(self.change_img_src_in_text(convert['content'], True)),
			'post_title': convert['title'],
			'post_excerpt': self.replace_url_src_to_url_target(self.change_img_src_in_text(get_value_by_key_in_dict(convert, 'short_content', ''), True)),
			'post_status': "publish" if convert['status'] else "private",
			'comment_status': convert.get('comment_status', 'open'),
			'ping_status': convert.get('ping_status', 'closed'),
			'post_password': '',
			'post_name': convert['url_key'],
			'to_ping': '',
			'pinged': '',
			'post_modified': convert['updated_at'] if convert['updated_at'] else get_current_time(),
			'post_modified_gmt': convert['updated_at'] if convert['updated_at'] else get_current_time(),
			'post_content_filtered': '',
			'post_parent': 0,
			'guid': self._notice['target']['cart_url'] + "/?p=",
			'menu_order': convert.get('menu_order', 0),
			'post_type': "post",
			'post_mime_type': '',
			'comment_count': 0
		}
		blog_query = self.create_insert_query_connector('posts', blog_data)
		blog_import = self.import_blog_data_connector(blog_query, True, convert['id'])
		if not blog_import:
			return response_error()
		self.insert_map(self.TYPE_BLOG, convert['id'], blog_import, convert['code'])
		return response_success(blog_import)

	def after_blog_import(self, blog_id, convert, blog, blogs_ext):
		all_queries = list()
		all_queries.append(self.create_update_query_connector('posts', {'guid': self._notice['target']['cart_url'] + "/?p=" + to_str(blog_id)}, {'ID': blog_id}))
		if convert['categories']:
			all_categories = list()
			for category in convert['categories']:
				category_id = self.import_category_blog(category)
				if not category_id:
					continue
				# language_code = category.get('language_code')
				if category_id:
					all_categories.append(category_id)
			all_categories = list(set(all_categories))
			for cate_id in all_categories:
				term_relationships_data = {
					'object_id': blog_id,
					'term_taxonomy_id': cate_id,
					'term_order': 0
				}
				category_query = self.create_insert_query_connector("term_relationships", term_relationships_data)
				all_queries.append(category_query)
		if convert['tags'] and isinstance(convert['tags'], dict) or ',' in convert['tags']:
			tags_src = convert['tags'].split(',')
		else:
			tags_src = list()
			tags_src.append(convert['tags'])
		if tags_src:
			check_exist_term_query = {
				'terms': {
					'type': 'select',
					'query': "SELECT * FROM _DBPRF_terms WHERE name IN " + self.list_to_in_condition(tags_src)
				},
				'post_tag': {
					'type': 'select',
					'query': "SELECT * FROM _DBPRF_terms AS t JOIN _DBPRF_term_taxonomy As tt ON t.term_id = tt.term_id WHERE tt.taxonomy = 'post_tag' AND t.name IN " + self.list_to_in_condition(tags_src)
				}
			}
			check_exist_term = self.select_multiple_data_connector(check_exist_term_query)
			check_exist_term_data = dict()
			check_exist_tag_data = dict()
			if check_exist_term['result'] == 'success' and check_exist_term['data']:
				if check_exist_term['data']['post_tag']:
					for check_exist_tag_row in check_exist_term['data']['post_tag']:
						check_exist_tag_data[check_exist_tag_row['name']] = check_exist_tag_row['term_taxonomy_id']
				if check_exist_term['data']['terms']:
					for check_exist_term_row in check_exist_term['data']['terms']:
						check_exist_term_data[check_exist_term_row['name']] = check_exist_term_row['term_id']
			for tag_src in tags_src:
				if check_exist_tag_data and tag_src in check_exist_tag_data:
					tag_id = check_exist_tag_data[tag_src]
				else:
					if check_exist_term_data and tag_src in check_exist_term_data:
						term_value_id = check_exist_term_data[tag_src]
					else:
						value_term = {
							'name': tag_src,
							'slug': self.sanitize_title(tag_src),
							'term_group': 0,
						}
						value_term_query = self.create_insert_query_connector('terms', value_term)
						term_value_id = self.import_data_connector(value_term_query, 'blogs')
					if not term_value_id:
						continue

					value_term_taxonomy = {
						'term_id': term_value_id,
						'taxonomy': "post_tag",
						'description': tag_src,
						'parent': 0,
						'count': 0
					}
					tag_id = self.import_data_connector(
						self.create_insert_query_connector('term_taxonomy', value_term_taxonomy), 'blogs')

				if tag_id:
					relationship = {
						'object_id': blog_id,
						'term_taxonomy_id': tag_id,
						'term_order': 0
					}
					all_queries.append(self.create_insert_query_connector('term_relationships', relationship))
		thumbnail_id = False
		if convert['thumb_image']['url'] or convert['thumb_image']['path']:
			image_process = self.process_image_before_import(convert['thumb_image']['url'], convert['thumb_image']['path'])
			image_import_path = self.uploadImageConnector(image_process, self.add_prefix_path(self.make_woocommerce_image_path(image_process['path']), self._notice['target']['config']['image_product'].rstrip('/')))
			if image_import_path:
				product_image = self.remove_prefix_path(image_import_path, self._notice['target']['config']['image_product'])
				image_details = self.get_sizes(image_process['url'])
				thumbnail_id = self.wp_image(product_image, image_details)
		postmeta = dict()
		if thumbnail_id:
			postmeta['_thumbnail_id'] = thumbnail_id

		if convert.get('content_frontpage') == 1:
			query_wpseo = {
				'type': 'select',
				'query': "SELECT * FROM `_DBPRF_options` WHERE `option_name` = 'sticky_posts'"
			}
			options_data = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query_wpseo)})
			if options_data :
				option_value = php_unserialize(options_data['data'][0]['option_value'])

				if option_value and isinstance(option_value, dict):
					option_value[to_len(option_value)] = blog_id
					#option_value.append(blog_id)
					data_set = {
						'option_value': php_serialize(option_value)
					}
					where = {
						'option_id': options_data['data'][0]['option_id'],
						'option_name': 'sticky_posts'
					}
					update_query = self.create_update_query_connector('options', data_set, where)
					all_queries.append(update_query)

				else:
					options_value = list()
					options_value.append(blog_id)
					data_set = {
						'option_value': php_serialize(options_value),
					}
					where = {
						'option_id': options_data['data'][0]['option_id'],
						'option_name': 'sticky_posts'
					}
					update_query = self.create_update_query_connector('options', data_set, where)
					all_queries.append(update_query)

		for meta_key, value in postmeta.items():
			postmeta_data = {
				'post_id': blog_id,
				'meta_key': meta_key,
				'meta_value': value
			}
			all_queries.append(self.create_insert_query_connector('postmeta', postmeta_data))

		if convert.get('review'):
			rv_status = {
				'2': 0,  # pedding
				'1': 1,  # approved
				'3': 'spam',  # not approved
				'0': 0
			}
			for review in convert['review']:
				customer_id = 0
				if review['customer']['id'] or review['customer']['code']:
					customer_id = self.get_map_field_by_src(self.TYPE_CUSTOMER, review['customer']['id'])
					if not customer_id:
						customer_id = 0
				review_parent_id = 0
				if review.get('parent_id') and to_int(review.get('parent_id')) != 0:
					review_parent_id = self.get_map_field_by_src(self.TYPE_REVIEW, review['parent_id'])
					if not review_parent_id:
						review_parent_id = 0
				review_data = {
					'comment_post_ID': blog_id,
					'comment_author': review['customer']['name'],
					'comment_author_email': review['customer']['code'],
					'user_id': customer_id,
					'comment_parent': review_parent_id,
					'comment_type': '',
					'comment_agent': '',
					'comment_approved': rv_status.get(str(review['status']), 'spam'),
					'comment_karma': 0,
					'comment_content': review['content'],
					'comment_date': review.get('created_at') if review.get('created_at') else get_current_time(),
					'comment_date_gmt': review.get('updated_at') if review.get('updated_at') else get_current_time(),
				}
				review_id = self.import_review_data_connector(self.create_insert_query_connector('comments', review_data), 'review')
				if review_id:
					self.insert_map(self.TYPE_REVIEW, review['id'], review_id, review['code'])

		self.import_multiple_data_connector(all_queries, 'blog')
		return response_success()

	def import_category_blog(self, convert):
		check_import = self.get_map_field_by_src(self.TYPE_CATEGORY_BLOG, convert['id'], convert['code'])
		if check_import:
			return check_import
		category_id = None
		category_import = self.category_import(convert, None, None)
		if category_import:
			self.after_category_import(category_import, convert, None, None)
			category_id = category_import['data']
		return category_id

	def addition_blog_import(self, convert, blog, blogs_ext):
		return response_success()

	# TODO: PAGE
	def prepare_pages_import(self):
		return self

	def prepare_pages_export(self):
		return self

	def get_pages_main_export(self):
		id_src = self._notice['process']['pages']['id_src']
		limit = self._notice['setting']['pages']
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_posts WHERE post_type = 'page' AND "
					 "ID > " + to_str(id_src) + " ORDER BY ID ASC LIMIT " + to_str(limit)
		}
		pages = self.select_data_connector(query, 'pages')
		if not pages or pages['result'] != 'success':
			return response_error()
		return pages

	def get_pages_ext_export(self, pages):
		page_ids = duplicate_field_value_from_list(pages['data'], 'ID')
		page_id_con = self.list_to_in_condition(page_ids)
		pages_ext_queries = {
			'postmeta': {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_postmeta WHERE post_id IN " + page_id_con
			},
		}
		pages_ext = self.select_multiple_data_connector(pages_ext_queries, 'pages')
		if not pages_ext or pages_ext['result'] != 'success':
			return response_error()
		thumbnail_id_list = get_list_from_list_by_field(pages_ext['data']['postmeta'], 'meta_key', '_thumbnail_id')
		thumbnail_ids = duplicate_field_value_from_list(thumbnail_id_list, 'meta_value')
		all_image_ids_query = self.list_to_in_condition(thumbnail_ids)
		page_ext_rel_queries = {
			'images': {
				'type': 'select',
				'query': "SELECT p.ID, p.post_title, pm.meta_value FROM _DBPRF_posts AS p LEFT JOIN _DBPRF_postmeta AS pm ON pm.post_id = p.ID AND pm.meta_key = '_wp_attached_file' WHERE p.ID IN " + all_image_ids_query,
			}
		}
		pages_ext_rel = self.select_multiple_data_connector(page_ext_rel_queries, 'pages')
		if (not pages_ext_rel) or pages_ext_rel['result'] != 'success':
			return response_error()
		pages_ext = self.sync_connector_object(pages_ext, pages_ext_rel)
		return pages_ext

	def convert_page_export(self, page, pages_ext):
		page_data = self.construct_cms_page()
		page_data['id'] = page['ID']
		page_data['title'] = page['post_title'] if page['post_title'] else (page['post_name'] if page['post_name'] else 'blog-' + to_str(page['ID']))
		page_data['short_description'] = ''
		page_data['content'] = page['post_content']
		page_data['description'] = page['post_content']
		page_data['short_content'] = page['post_excerpt']
		page_data['url_key'] = page['post_name']
		page_data['parent_id'] = page['post_parent']
		page_data['status'] = True if page['post_status'] == 'publish' else False
		page_data['created_at'] = convert_format_time(page['post_date'])
		page_data['sort_order'] = page['menu_order']
		page_data['comment_status'] = page['comment_status']
		page_meta = get_list_from_list_by_field(pages_ext['data']['postmeta'], 'post_id', page['ID'])
		thumbnail_id = self.get_value_metadata(page_meta, '_thumbnail_id', 0)
		if thumbnail_id:
			thumbnail_src = get_list_from_list_by_field(pages_ext['data']['images'], 'ID', thumbnail_id)
			if thumbnail_src:
				img_data = dict()
				img_data['path'] = thumbnail_src[0]['meta_value'].lstrip('/')
				img_data['label'] = thumbnail_src[0]['post_title']
				img_data['url'] = self._notice['src']['cart_url'].rstrip('/') + '/wp-content/uploads/'
				page_data['images'].append(img_data)
		return response_success(page_data)

	def get_page_id_import(self, convert, page, pages_ext):
		return page['ID']

	def check_page_import(self, convert, page, pages_ext):
		return True if self.get_map_field_by_src(self.TYPE_PAGE, convert['id']) else False

	def router_page_import(self, convert, page, pages_ext):
		return response_success('page_import')

	def before_page_import(self, convert, page, pages_ext):
		return response_success()

	def page_import(self, convert, page, pages_ext):

		code_name = convert['title']
		code_name = self.sanitize_title(code_name).strip('-')
		check_slug_exist = True
		while check_slug_exist:
			check_slug_exist = True if self.select_map(self._migration_id, self.TYPE_PAGE, None, None, None, code_name) else False
			if check_slug_exist:
				if get_value_by_key_in_dict(convert, 'sku', ''):
					code_name += to_str(get_value_by_key_in_dict(convert, 'sku', ''))
				else:
					code_name += to_str(get_value_by_key_in_dict(convert, 'id', ''))
				code_product = code_name.replace(' ', '-')
		parent_id = self.get_map_field_by_src(self.TYPE_PAGE, to_int(convert['parent_id']))
		if not parent_id:
			parent_id = 0
		data = {
			'post_author': 1,
			'post_date': convert['created_at'] if convert['created_at'] and '0000-00-00' not in convert['created_at'] else get_current_time(),
			'post_date_gmt': convert['created_at'] if convert['created_at'] and '0000-00-00' not in convert['created_at'] else get_current_time(),
			'post_content': convert['content'] if convert['content'] else "",
			'post_title': convert['title'],
			'post_status': 'publish' if convert['status'] else 'trash',
			'comment_status': convert.get('comment_status', 'open'),
			'ping_status': 'open',
			'post_name': code_name[:200],
			'post_modified': convert['created_at'] if convert['created_at'] and '0000-00-00' not in convert['created_at'] else get_current_time(),
			'post_modified_gmt': convert['created_at'] if convert['created_at'] and '0000-00-00' not in convert['created_at'] else get_current_time(),
			'post_parent': parent_id,
			'post_type': 'page',
			'comment_count': 0,
			'guid': '',
			'post_excerpt': '',
			'to_ping': '',
			'pinged': '',
			'post_content_filtered': '',
			'menu_order': get_value_by_key_in_dict(convert, 'sort_order', 0)
		}
		page_query = self.create_insert_query_connector('posts', data)
		page_id = self.import_page_data_connector(page_query, True, convert['id'])
		if not page_id:
			return response_error('Page ' + to_str(convert['id']) + ' import false.')
		self.insert_map(self.TYPE_PAGE, convert['id'], page_id, convert['title'])
		return response_success(page_id)

	def after_page_import(self, page_id, convert, page, pages_ext):
		data = {
			'guid': self._notice['target']['cart_url'] + '?p=' + str(page_id)
		}
		where_id = {
			'id': page_id
		}
		update_query = self.create_update_query_connector('posts', data, where_id)
		self.import_data_connector(update_query, 'page')
		data_meta = {
			'post_id': page_id,
			'meta_key': '_edit_lock',
			'meta_value': int(time.time()),
		}
		self.import_page_data_connector(self.create_insert_query_connector('postmeta', data_meta), True, convert['id'])
		if self._notice['target']['support']['yoast_seo']:
			page_meta = {
				'_yoast_wpseo_title': to_str(convert['meta_title']).replace('%', '') if 'meta_title' in convert and convert['meta_title'] else '',
				'_yoast_wpseo_metadesc': to_str(convert['meta_description']).replace('%', '') if 'meta_description' in convert and convert['meta_description'] else '',
			}
			for meta_key, meta_value in page_meta.items():
				meta_insert = {
					'post_id': page_id,
					'meta_key': meta_key,
					'meta_value': meta_value
				}
				self.import_page_data_connector(self.create_insert_query_connector("postmeta", meta_insert))
		thumbnail_id = False
		if convert['images']:
			for image in convert['images']:
				image_process = self.process_image_before_import(image['url'], image.get('path', ''))
				image_import_path = self.uploadImageConnector(image_process, self.add_prefix_path(self.make_woocommerce_image_path(image_process['path']), self._notice['target']['config']['image_product'].rstrip('/')))
				if image_import_path:
					product_image = self.remove_prefix_path(image_import_path, self._notice['target']['config']['image_product'])
					image_details = self.get_sizes(image_process['url'])
					thumbnail_id = self.wp_image(product_image, image_details)
				postmeta = dict()
				if thumbnail_id:
					postmeta['_thumbnail_id'] = thumbnail_id
				for meta_key, value in postmeta.items():
					postmeta_data = {
						'post_id': page_id,
						'meta_key': meta_key,
						'meta_value': value
					}
					self.import_page_data_connector(self.create_insert_query_connector('postmeta', postmeta_data), True, convert['id'])
		data_revision = {
			'post_author': 1,
			'post_date': convert['created_at'] if convert['created_at'] and '0000-00-00' not in convert['created_at'] else get_current_time(),
			'post_date_gmt': convert['created_at'] if convert['created_at'] and '0000-00-00' not in convert['created_at'] else get_current_time(),
			'post_content': convert['content'],
			'post_title': convert['title'],
			'post_status': 'inherit',
			'comment_status': 'closed',
			'ping_status': 'closed',
			'post_name': str(page_id) + '-revision-v1',
			'post_modified': convert['created_at'] if convert['created_at'] and '0000-00-00' not in convert['created_at'] else get_current_time(),
			'post_modified_gmt': convert['created_at'] if convert['created_at'] and '0000-00-00' not in convert['created_at'] else get_current_time(),
			'post_parent': page_id,
			'menu_order': get_value_by_key_in_dict(convert, 'sort_order', 0),
			'post_type': 'revision',
			'comment_count': 0,
			'guid': self._notice['target']['cart_url'] + '/2019/08/27/' + str(page_id) + '-revision-v1',
			'post_excerpt': '',
			'to_ping': '',
			'pinged': '',
			'post_content_filtered': ''
		}
		self.import_page_data_connector(self.create_insert_query_connector('posts', data_revision), True, convert['id'])
		return response_success()

	def addition_page_import(self, convert, page, pages_ext):
		return response_success()

	#super cate
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
						 "WHERE tx.taxonomy = '" + taxonomy_type + "' AND tx.term_id IN " + self.list_to_in_condition(parent_ids)
			}

		}
		if cart_version > 223:
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
		if 'wpml' in self._notice['target']['support'] and self._notice['target']['support']['wpml']:
			categories_ext_queries['icl_translations'] = {
				'type': 'select',
				'query': "SELECT * FROM _DBPRF_icl_translations WHERE element_type = 'tax_product_cat' and element_id IN " + self.list_to_in_condition(
					category_ids)
			}

		# categories_ext = self.get_connector_data(url_query, {
		# 	'serialize': True,
		# 	'query': json.dumps(categories_ext_queries)
		# })
		categories_ext = self.get_connector_data(url_query, {
			'serialize': True,
			'query': json.dumps(categories_ext_queries)
		})

		if not categories_ext or categories_ext['result'] != 'success':
			return response_warning()

		thumb_id_list = get_list_from_list_by_field(categories_ext['data']['woocommerce_termmeta'], 'meta_key', 'thumbnail_id')
		thumbnail_ids = duplicate_field_value_from_list(thumb_id_list, 'meta_value')
		thumb_ids_query = self.list_to_in_condition(thumbnail_ids)
		categories_ext_rel_queries = {
			'post_meta': {
				'type': 'select',
				'query': "SELECT p.ID, p.post_title, pm.meta_value, p.guid FROM _DBPRF_posts AS p "
						 "LEFT JOIN _DBPRF_postmeta AS pm ON pm.post_id = p.ID AND pm.meta_key = '_wp_attached_file' WHERE p.ID IN " + thumb_ids_query
			}
		}
		if 'wpml' in self._notice['target']['support'] and self._notice['target']['support']['wpml']:
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

		categories_ext['is_blog'] = True
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
		if cart_version > 223:
			category_src = get_list_from_list_by_field(categories_ext['data']['woocommerce_termmeta'], 'term_id', category['term_id'])
		else:
			category_src = get_list_from_list_by_field(categories_ext['data']['woocommerce_termmeta'], 'woocommerce_term_id', category['term_id'])

		if category_src:
			category_img_id = self.get_value_metadata(category_src, 'thumbnail_id', 0)
			img_meta = get_list_from_list_by_field(categories_ext['data']['post_meta'], 'ID', category_img_id)
			if img_meta:
				img_label = img_meta[0]['post_title']
				category_path = img_meta[0]['meta_value']
				category_url = img_meta[0]['guid'].replace(img_meta[0]['meta_value'], '')

		category_data['id'] = category['term_id']
		category_data['code'] = category['slug']
		category_data['name'] = category['name'] if category.get('name') else ''
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
		if 'wpml' in self._notice['target']['support'] and self._notice['target']['support']['wpml']:
			trid = get_row_value_from_list_by_field(categories_ext['data']['icl_translations'], 'element_id', category['term_taxonomy_id'], 'trid')
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
			if 'product_cat' in option_value:
				if to_int(category['term_id']) in option_value['product_cat']:
					category_data['meta_description'] = get_value_by_key_in_dict(
						option_value['product_cat'][to_int(category['term_id'])], 'wpseo_desc', '')
					category_data['meta_keyword'] = get_value_by_key_in_dict(
						option_value['product_cat'][to_int(category['term_id'])], 'wpseo_focuskw', '')
		return response_success(category_data)

	def get_category_parent(self, parent_id):
		type_map = self.TYPE_CATEGORY if not self.blog_running else self.TYPE_CATEGORY_BLOG
		category_exist = self.select_map(self._migration_id, type_map, parent_id)
		if category_exist:
			return response_success({
				'id': parent_id,
				'code': '',
				'is_blog': True,
			})
		taxonomy_type = 'product_cat' if not self.blog_running else 'category'
		query = {
			'type': 'select',
			'query': "SELECT * FROM _DBPRF_term_taxonomy as tx LEFT JOIN _DBPRF_terms AS t ON t.term_id = tx.term_id "
			         "WHERE tx.taxonomy = '"+ taxonomy_type +"' AND tx.term_id = " + to_str(parent_id)
		}
		categories = self.get_connector_data(self.get_connector_url('query'), {'query': json.dumps(query)})
		if not categories or categories['result'] != 'success':
			return response_error('could not get category parent to export')
		if categories and categories['data']:
			category = categories['data'][0]
			if type_map == 'category_blog':
				categories['is_blog'] = True
			categories_ext = self.get_categories_ext_export(categories)
			if type_map == 'category_blog':
				categories_ext['is_blog'] = True
			category_convert = self.convert_category_export(category, categories_ext)
			return category_convert
		return response_error('could not get category parent to export')

	def finish_blog_import(self):
		self.blog_running = False
		return response_success()

	def sanitize_title(self, str_convert = '', char = 200, suffix = 'html'):
		new_suffix = ''
		suffix = to_str(suffix)
		if suffix:
			if '.' not in suffix:
				suffix = '.' + suffix
			if to_len(str_convert) > to_len(suffix) and str_convert[-to_len(suffix):] == suffix:
				new_suffix = suffix
				str_convert = str_convert[0:-to_len(suffix)]
				if char:
					char -= to_len(new_suffix)
		res = self.convert_attribute_code(str_convert)
		if char:
			index = 0
			while to_len(res) > char:
				index -= 1
				res = self.convert_attribute_code(str_convert[0:index])

		return res + new_suffix

	def make_woocommerce_image_path(self, image, image_type = 'product'):
		image_name = os.path.basename(image)
		return get_current_time('%Y/%m') + '/' + image_name

	def wp_image(self, path, image_details = None, label = '', convert = dict):
		url = self._notice['target']['cart_url'].rstrip('/') + '/' + self._notice['target']['config']['image'].rstrip('/') + '/' + path.lstrip('/')
		image_id = self.get_map_field_by_src(self.WP_IMAGE, None, url)
		if image_id:
			# check = self.select_data_connector(self.create_select_query_connector('posts', {'ID': image_id, 'post_type': 'attachment'}))
			# if check and check['result'] == 'success' and check['data']:
			return image_id
			# else:
			# 	self.delete_obj(TABLE_MAP, {'migration_id': self._migration_id, 'type': self.TYPE_IMAGE, 'id_desc': image_id})
		mime_type = mimetypes.guess_type(path)[0]
		title = ''
		post_data = {
			'post_author': 1,
			'post_date': convert['created_at'] if isinstance(convert, dict) and convert.get('created_at') else get_current_time(),
			'post_date_gmt': get_current_time(),
			'post_content': label if label else '',
			'post_title': title if title else 'images',
			'post_excerpt': label if label else '',
			'post_status': 'inherit',
			'comment_status': 'open',
			'ping_status': 'open',
			'post_name': self.basename(path),
			'to_ping': '',
			'pinged': '',
			'post_modified': convert['updated_at'] if isinstance(convert, dict) and convert.get('updated_at') else get_current_time(),
			'post_modified_gmt': get_current_time(),
			'post_content_filtered': '',
			'post_parent': 0,
			'guid': url,
			'menu_order': 0,
			'post_type': 'attachment',
			'post_mime_type': mime_type if mime_type else ('image/png' if '.png' in url else 'image/jpeg')
		}
		image_query = self.create_insert_query_connector('posts', post_data)
		post_id = self.import_data_connector(image_query)

		if not post_id:
			return False
		self.insert_map(self.WP_IMAGE, None, post_id, url)
		all_queries = list()
		meta = {
			'post_id': post_id,
			'meta_key': '_wp_attached_file',
			'meta_value': path.lstrip('/')
		}
		image_meta_query = self.create_insert_query_connector('postmeta', meta)
		all_queries.append(image_meta_query)

		attachment_metadata = {
			'post_id': post_id,
			'meta_key': '_wp_attachment_metadata',
			'meta_value': php_serialize({
				'file': path.lstrip('/'),
				'width': self.get_image_size(),
				'height': self.get_image_size(),
			})
		}
		attachment_metadata_query = self.create_insert_query_connector('postmeta', attachment_metadata)
		all_queries.append(attachment_metadata_query)

		attachment_image_alt = {
			'post_id': post_id,
			'meta_key': '_wp_attachment_image_alt',
			'meta_value': label
		}
		attachment_image_alt_query = self.create_insert_query_connector('postmeta', attachment_image_alt)

		all_queries.append(attachment_image_alt_query)
		if all_queries:
			self.import_multiple_data_connector(all_queries, 'wp_image')
		return post_id

	def get_sizes(self, url):
		return ''
		req = Request(url, headers = {'User-Agent': 'Mozilla/5.0'})
		try:
			file = urlopen(req)
		except:
			self.log('image: ' + to_str(url) + ' 404', 'image_error')
			return False, False
		size = file.headers.get("content-length")
		# date = datetime.strptime(file.headers.get('date'), '%a, %d %b %Y %H:%M:%S %Z')
		# type = file.headers.get('content-type')
		if size: size = to_int(size)
		p = ImageFile.Parser()
		while 1:
			data = file.read(1024)
			if not data:
				break
			p.feed(data)
			if p.image:
				return size, p.image.size
				break
		file.close()
		return size, False

	def basename(self, path, suffix = None):
		basename = os.path.basename(path)
		if suffix and basename.endswith(suffix):
			basename = basename[:-len(suffix)]
		return basename

	def product_to_in_condition_linked(self, ids):
		if not ids:
			return "('null')"
		result = "LIKE '%:" + ";%' OR meta_value LIKE '%:".join([str(id) for id in ids]) + ";%'"
		return result

	def mime_content_type_custom(self, filename):
		mime_types = {
			'txt': 'text/plain',
			'htm': 'text/html',
			'html': 'text/html',
			'php': 'text/html',
			'css': 'text/css',
			'js': 'application/javascript',
			'json': 'application/json',
			'xml': 'application/xml',
			'swf': 'application/x-shockwave-flash',
			'flv': 'video/x-flv',
			# images
			'png': 'image/png',
			'jpe': 'image/jpeg',
			'jpeg': 'image/jpeg',
			'jpg': 'image/jpeg',
			'gif': 'image/gif',
			'bmp': 'image/bmp',
			'ico': 'image/vnd.microsoft.icon',
			'tiff': 'image/tiff',
			'tif': 'image/tiff',
			'svg': 'image/svg+xml',
			'svgz': 'image/svg+xml',
			# archives
			'zip': 'application/zip',
			'rar': 'application/x-rar-compressed',
			'exe': 'application/x-msdownload',
			'msi': 'application/x-msdownload',
			'cab': 'application/vnd.ms-cab-compressed',
			# audio / video
			'mp3': 'audio/mpeg',
			'qt': 'video/quicktime',
			'mov': 'video/quicktime',
			# adobe
			'pdf': 'application/pdf',
			'psd': 'image/vnd.adobe.photoshop',
			'ai': 'application/postscript',
			'eps': 'application/postscript',
			'ps': 'application/postscript',
			# ms office
			'doc': 'application/msword',
			'rtf': 'application/rtf',
			'xls': 'application/vnd.ms-excel',
			'ppt': 'application/vnd.ms-powerpoint',
			# open office
			'odt': 'application/vnd.oasis.opendocument.text',
			'ods': 'application/vnd.oasis.opendocument.spreadsheet',
		}
		file_tmp = filename.split(',')
		file_name = file_tmp.pop()
		ext = file_name.lower()
		if ext in mime_types:
			return mime_types[ext]
		else:
			return 'application/octet-stream'

	def uniqid(self, prefix):
		return prefix + hex(int(time.time()))[2:10] + hex(int(time.time() * 1000000) % 0x100000)[2:7]

	def get_value_metadata(self, meta, meta_key, default_value = None):
		row = get_row_from_list_by_field(meta, 'meta_key', meta_key)
		value = default_value
		if row:
			value = get_value_by_key_in_dict(row, 'meta_value', default_value)
		return value

	# def generate_url(self, title):
	# 	if not title:
	# 		return ''
	# 	special = {
	# 		'Æ': 'AE',
	# 		'Đ': 'd',
	# 		'Ø': 'O',
	# 		'Þ': 'TH',
	# 		'ß': 'ss',
	# 		'æ': 'ae',
	# 		'ð': 'd',
	# 		'ø': 'o',
	# 		'þ': 'th',
	# 		'Œ': 'OE',
	# 		'œ': 'oe',
	# 		'ƒ': 'f'
	# 	}
	# 	for index, val in special.items():
	# 		title = title.replace(index, val)

		# text = unicodedata.normalize('NFKD', title).encode('ascii', 'ignore')
		# return text.decode() if text.decode() not in ['', '/', ' / '] else title
		# return title

	def import_category_parent(self, convert_parent, lang_code = None):
		category_type = self.TYPE_CATEGORY
		if convert_parent.get('is_blog'):
			category_type = self.TYPE_CATEGORY_BLOG
		parent_exists = self.get_map_field_by_src(category_type, convert_parent['id'], convert_parent['code'], lang_code)
		if parent_exists:
			return response_success(parent_exists)
		category = get_value_by_key_in_dict(convert_parent, 'category', dict())
		categories_ext = get_value_by_key_in_dict(convert_parent, 'categories_ext', dict())
		category_parent_import = self.category_import(convert_parent, category, categories_ext)
		self.after_category_import(category_parent_import['data'], convert_parent, category, categories_ext)
		return category_parent_import
	def get_image_size(self):
		if self.image_size:
			return self.image_size
		res = self.select_data_connector(self.create_select_query_connector('options', {'option_name': 'woocommerce_single_image_width'}))
		if res and res['result'] == 'success' and res['data']:
			self.image_size = to_int(res['data'][0]['option_value'])
		else:
			self.image_size = 600
		return self.image_size

	def is_exist_lecm_rewrite(self):
		if self.exist_lecm_rewrite is not None:
			return self.exist_lecm_rewrite
		query = {
			'type': 'select',
			'query': "SHOW TABLES LIKE '_DBPRF_lecm_rewrite';"
		}
		check_table_exit = self.select_data_connector(query, 'product')
		if check_table_exit['result'] == 'success' and to_len(check_table_exit['data']) > 0:
			self.exist_lecm_rewrite = True
			return True
		self.exist_lecm_rewrite = False
		return False

	def get_cookie(self, set_cookie):
		cookie = str(set_cookie).split(',')
		list_cookie = list()
		for cookie_row in cookie:
			split_cookie = cookie_row.split(';')
			list_cookie.append(split_cookie[0])
		request_cookie = '; '.join(list_cookie)
		return request_cookie

	def set_referer_to_headers(self, headers, referer):
		headers['Referer'] = referer
		return headers

	def install_module_connector(self, request):
		parent = super().install_module_connector(request)
		if parent['result'] != 'success':
			return parent
		key_api = self._type + "_api"
		elm_module_connector = '#src-api-error'
		elm_cart_url = '#source-cart-url'
		if self._type == 'target':
			elm_module_connector = '#target-api-error'
			elm_cart_url = '#target-cart-url'
		if not self._cart_url or not request.get(key_api) or not request[key_api].get('account') or not request[key_api].get('password'):
			return response_error('Info Invalid. Please enter the correct url, username and password', elm_module_connector, 'Info Invalid')
		wp_url = self.format_url(self._cart_url)
		wp_login = wp_url + "/wp-login.php"
		wp_admin = wp_url + "/wp-admin"
		wp_install = wp_admin + "/plugin-install.php?s=litextension&tab=search&type=term"
		wp_install_connector = wp_admin + "/admin.php?page=install-connector&token=" + (request.get('src_token') if self._type == 'src' else request.get('target_token'))
		username = request[key_api]['account']
		password = request[key_api]['password']
		with requests.Session() as wp_session:
			try:
				if self._notice[self.get_type()]['config'].get('auth'):
					wp_session.auth = (self._notice[self.get_type()]['config']['auth'].get('user'), self._notice[self.get_type()]['config']['auth'].get('pass'))
				headers = {
					'Cookie': 'wordpress_test_cookie=WP Cookie check',
					'Referer': wp_login,
					'User-Agent': self.get_random_useragent()
				}
				data_login = {
					'log': username,
					'pwd': password,
					'wp-submit': 'Log In',
					'redirect_to': wp_admin,
					'testcookie': '1'
				}
				login_page = wp_session.get(wp_login, headers = headers)
				if login_page.status_code == 404:
					response = response_error('We can not access {}, Please <a href="#" hide="#{}-module-connector-auto" show="#{}-module-connector-manually" class="js-btn-module-connector">click</a> to manually upload connector!'.format(url_to_link(wp_login), self._type, self._type), elm_cart_url, 'Url invalid')
					scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(self._cart_url)
					if path:
						root_url = self.detect_root_url()
						if root_url:
							response['msg'] = self.error_root_url(root_url)
					return response

				login_page_content = BeautifulSoup(login_page.text, 'lxml')
				post_login_url = login_page_content.find("form", {"id": "loginform"})
				post_login_url = post_login_url['action']
				login = wp_session.post(post_login_url, headers = headers, data = data_login, allow_redirects = False)
				if login.status_code not in [200, 302]:
					login = wp_session.post(post_login_url, headers = headers, data = data_login, allow_redirects = False, proxies = self.convert_proxy_request(self.PROXY_HOST))

				login_content = BeautifulSoup(login.text, 'lxml')

				login_error = login_content.find('div', {'id': 'login_error'})
				if login_error:
					return response_error('The account sign-in was incorrect or your account is disabled temporarily. Please enter correct account wordpress admin!', elm_module_connector, 'Account incorrect')

				headers['Cookie'] = self.get_cookie(login.headers.get("set-cookie"))

				wp_session.get(wp_admin, headers = headers)
				headers = self.set_referer_to_headers(headers, wp_admin)
				plugin_install = wp_session.get(wp_install, headers = headers, allow_redirects = False)
				if plugin_install.status_code in [301, 302]:
					redirect_url = plugin_install.headers.get('location')
					if redirect_url:
						wp_admin = redirect_url.replace('plugin-install.php', '')
						wp_install = wp_admin + "/plugin-install.php?s=litextension&tab=search&type=term"
						plugin_install = wp_session.get(wp_install, headers = headers, allow_redirects = False)

				plugin_install_content = BeautifulSoup(plugin_install.text, 'lxml')
				plugin_connector = plugin_install_content.find("div", {"class": "plugin-card-litextension-data-migration-to-woocommerce"})

				action_install = plugin_connector.find_all("a", {"class": "install-now"})
				if action_install:
					link_install = action_install[0]['href']
					install = wp_session.get(link_install, headers = headers)
					install_content = BeautifulSoup(install.text, 'lxml')
					if install_content.find("div", {'id': 'request-filesystem-credentials-form'}):
						return response_error('Failed to automatically install the plugin. Please disable FTP Credentials request following the steps from <a href = "https://litextension.com/faq/docs/userguide-demo/connector/i-keep-getting-ftp-access-required-message/" target = "_blank">this Guide</a> or <a href="#" hide="#{}-module-connector-auto" show="#{}-module-connector-manually" type_upload = "connector" class="js-btn-module-connector">click</a> to manually upload connector!'.format(self._type, self._type), elm_cart_url, 'FTP access required.')

					link_active = install_content.find_all('a', {'target': '_parent', 'class': 'button-primary'})
					if not link_active:
						return response_error('Failed to automatically install the plugin. Please <a href="#" hide="#{}-module-connector-auto" show="#{}-module-connector-manually" class="js-btn-module-connector">click</a> to manually upload connector!'.format(self._type, self._type), elm_cart_url, 'Install failed.')

					url_active = wp_admin + "/" + link_active[0]['href']
					active = wp_session.get(url_active + "/" + link_active[0]['href'], headers = headers)
				action_active = plugin_connector.find_all("a", {"class": "activate-now"})
				if action_active:
					link_active = action_active[0]['href']
					active = wp_session.get(link_active, headers = headers)
				install_connector = wp_session.get(wp_install_connector, headers = headers)
				install_connector_content = BeautifulSoup(install_connector.text, 'lxml')
				if install_connector_content.find('body', {'id': "error-page"}):
					return response_error('Failed to active. Please login to wordpress admin, active plugin <b>LitExtension: Migrate Shopping Carts to WooCommerce</b>. After active, please <a href="{}" target="_blank">click</a> to install Connector and try again'.format(wp_install_connector), elm_cart_url, 'Active failed.')
				response = install_connector_content.find('p', {'id': "litextension-response"})
				if not response:
					return response_error('Failed to automatically install the connector. Please <a href="#" hide="#{}-module-connector-auto" show="#{}-module-connector-manually" class="js-btn-module-connector">click</a> to manually upload connector!'.format(self._type, self._type), elm_cart_url, 'Install failed.')
				response = response.getText()
				response = json_decode(response)
				if not response:
					return response_error('Failed to automatically install the connector. Please <a href="#" hide="#{}-module-connector-auto" show="#{}-module-connector-manually" class="js-btn-module-connector">click</a> to manually upload connector!'.format(self._type, self._type), elm_cart_url, 'Install failed.')
				if response['result'] != 'success':
					return response_error(response['msg'], elm_cart_url, 'Install failed.')
			except Exception:
				self.log_traceback('module_woo')
				return response_error('Failed to automatically install the connector. Please <a href="#" hide="#{}-module-connector-auto" show="#{}-module-connector-manually" class="js-btn-module-connector">click</a> to manually upload connector!'.format(self._type, self._type), elm_cart_url, 'Install failed.')

		return response_success()

	def get_path_connector(self):
		return "wp-content/plugins/litextension-data-migration-to-woocommerce"

	def get_link_change_token(self):
		wp_url = self.format_url(self._cart_url)
		return wp_url + "/wp-admin/admin.php?page=install-connector&token=" + self._notice[self._type]['config']['token']

	def format_url(self, url):
		url = super().format_url(url)
		filter_url = ['wp-admin', 'wp-login.php']
		for char in filter_url:
			find_char = url.find(char)
			if find_char >= 0:
				url = url[:find_char]
		return url.strip("/")

	def detect_root_url(self):
		scheme, netloc, path, qs, anchor = urllib.parse.urlsplit(self._cart_url)
		path = to_str(path).strip("/").split('/')
		if not path:
			return ''
		del path[-1]
		while True:
			new_path = '/'.join(path)
			url = urllib.parse.urlunsplit((scheme, netloc, new_path, qs, anchor))
			try:
				exist = requests.head(to_str(url).rstrip('/') + "/wp-login.php", headers={"User-Agent": self.get_random_useragent(), "Referer": 'https://google.com'}, timeout = 10, verify = False)
				if exist.status_code == requests.codes.ok:
					return url
			except Exception as e:
				pass
			if not path:
				break
			del path[-1]
		return ''

	def error_root_url(self, root_url):
		if root_url != self._cart_url:
			return 	'We can not access {}, but we detect {} is accessible, if {} is your root url please use it as Url'.format(url_to_link(self._cart_url + "/wp-login.php"), url_to_link(root_url + "/wp-login.php"), url_to_link(root_url))
		return ''