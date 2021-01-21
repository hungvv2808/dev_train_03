import os
from cartmigration.models.setup import Setup
file_config = 'cartmigration/etc/config.ini'
if os.path.isfile(file_config):
	print("File config is exist, setup db with file config")
	print("----------------------------------")
setup = Setup()
setup.run()