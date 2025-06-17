'''Configuration management for OCI Data Manager'''

import os
from pathlib import Path

import yaml
from oci import config as oci_config


class Config:
	'''Manages configuration for OCI services'''

	def __init__(self, config_path=None):
		'''
		Initialize configuration from YAML file

		Args:
			config_path: Path to config.yaml file. If None, looks for:
						1. Environment variable OCI_DATA_MANAGER_CONFIG
						2. config.yaml in current directory
						3. config.yaml in home directory
		'''
		self.config_path = self._find_config_file(config_path)
		self.config = self._load_config()
		self._validate_config()

	def _find_config_file(self, config_path):
		'''Find configuration file'''
		if config_path:
			return Path(config_path)

		# Check environment variable
		env_path = os.environ.get('OCI_DATA_MANAGER_CONFIG')
		if env_path:
			return Path(env_path)

		# Check current directory
		current_dir = Path('config.yaml')
		if current_dir.exists():
			return current_dir

		# Check home directory
		home_dir = Path.home() / 'config.yaml'
		if home_dir.exists():
			return home_dir

		raise FileNotFoundError(
			'No config.yaml found. Please create one from config.yaml.example'
		)

	def _load_config(self):
		'''Load configuration from YAML file'''
		with open(self.config_path, 'r') as f:
			return yaml.safe_load(f)

	def _validate_config(self):
		'''Validate required configuration keys'''
		required_oci = ['user', 'key_file', 'fingerprint', 'tenancy', 'region']
		required_storage = ['namespace']
		required_adw = ['connection_string', 'username', 'password']

		# Check OCI config
		if 'oci' not in self.config:
			raise ValueError('Missing "oci" section in config.yaml')
		for key in required_oci:
			if key not in self.config['oci']:
				raise ValueError(f'Missing required OCI config: {key}')

		# Check storage config
		if 'storage' in self.config:
			for key in required_storage:
				if key not in self.config['storage']:
					raise ValueError(f'Missing required storage config: {key}')

		# Check ADW config
		if 'adw' in self.config:
			for key in required_adw:
				if key not in self.config['adw']:
					raise ValueError(f'Missing required ADW config: {key}')

		# Expand key file path
		key_file = self.config['oci']['key_file']
		self.config['oci']['key_file'] = str(Path(key_file).expanduser())

	def get_oci_config(self):
		'''Get OCI SDK configuration dictionary'''
		return {
			'user': self.config['oci']['user'],
			'key_file': self.config['oci']['key_file'],
			'fingerprint': self.config['oci']['fingerprint'],
			'tenancy': self.config['oci']['tenancy'],
			'region': self.config['oci']['region'],
		}

	def get_storage_config(self):
		'''Get storage configuration'''
		return self.config.get('storage', {})

	def get_adw_config(self):
		'''Get ADW configuration'''
		return self.config.get('adw', {})