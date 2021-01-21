import os

# from cartmigration.libs.base_controller import BaseController, BASE_DIR, shutil
import shutil

from cartmigration.libs.utils import *

BASE_DIR = 'cartmigration'

def get_target_cart_name(notice):
	basecart = get_model('basecart')
	source_cart_type = notice['src']['cart_type']
	target_cart_type = notice['target']['cart_type']
	check = False
	if (source_cart_type == 'magento') and (target_cart_type == 'magento'):
		check = True
	cart_version = notice['target']['config']['version']
	cart_name = getattr(basecart, 'get_cart')(target_cart_type, cart_version, check)
	return cart_name


def get_source_cart_name(notice):
	basecart = get_model('basecart')
	source_cart_type = notice['src']['cart_type']
	target_cart_type = notice['target']['cart_type']
	check = False
	if (source_cart_type == 'magento') and (target_cart_type == 'magento'):
		check = True
	cart_version = notice['src']['config']['version']
	cart_name = getattr(basecart, 'get_cart')(source_cart_type, cart_version, check)
	return cart_name

def clone_code_for_migration_id(migration_id):
	if check_folder_clone(migration_id):
		return True
	base_dir = 'pub/' + DIR_PROCESS + migration_id
	if not os.path.isdir(base_dir):
		os.makedirs(base_dir)
	folder_copy = ['controllers', 'libs', 'models']
	file_copy = ['bootstrap.py', 'helper.py']
	for folder in folder_copy:
		if os.path.isdir(base_dir + '/' + BASE_DIR + '/' + folder):
			continue
		shutil.copytree(BASE_DIR + '/' + folder, base_dir + '/' + BASE_DIR + '/' + folder)
	for file in file_copy:
		if os.path.isfile(base_dir + '/' + file):
			continue
		shutil.copy(file, base_dir + '/' + file)

	git_ignore_file = base_dir + '/' + '.gitignore'
	f = open(git_ignore_file, "w+")
	f.write('*')
	change_permissions_recursive(base_dir, 0o777)


def change_permissions_recursive(path, mode):
	os.chmod(path, mode)
	for root, dirs, files in os.walk(path):
		for sub_dir in dirs:
			os.chmod(os.path.join(root, sub_dir), mode)
		for sub_file in files:
			os.chmod(os.path.join(root, sub_file), mode)

