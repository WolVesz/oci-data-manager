'''Object Storage bucket operations'''

import io
from pathlib import Path

import pandas as pd
from oci import object_storage

from oci_data_manager.config import Config


class StorageClient:
	'''Client for OCI Object Storage operations'''

	def __init__(self, config_path=None):
		'''
		Initialize Storage client

		Args:
			config_path: Path to config.yaml file
		'''
		self.config = Config(config_path)
		self.oci_config = self.config.get_oci_config()
		self.storage_config = self.config.get_storage_config()

		# Initialize OCI client
		self.client = object_storage.ObjectStorageClient(self.oci_config)
		self.namespace = self.storage_config['namespace']
		self.default_bucket = self.storage_config.get('default_bucket')

	def list_objects(self, bucket_name=None, prefix=None, limit=1000):
		'''
		List objects in a bucket

		Args:
			bucket_name: Bucket name (uses default if not provided)
			prefix: Filter objects by prefix
			limit: Maximum number of objects to return

		Returns:
			List of object names
		'''
		bucket = bucket_name or self.default_bucket
		if not bucket:
			raise ValueError('No bucket specified and no default bucket configured')

		kwargs = {
			'namespace_name': self.namespace,
			'bucket_name': bucket,
			'limit': limit,
		}
		if prefix:
			kwargs['prefix'] = prefix

		response = self.client.list_objects(**kwargs)
		return [obj.name for obj in response.data.objects]

	def read_object(self, object_name, bucket_name=None):
		'''
		Read an object from bucket

		Args:
			object_name: Name of the object
			bucket_name: Bucket name (uses default if not provided)

		Returns:
			bytes: Object content
		'''
		bucket = bucket_name or self.default_bucket
		if not bucket:
			raise ValueError('No bucket specified and no default bucket configured')

		response = self.client.get_object(
			namespace_name=self.namespace, bucket_name=bucket, object_name=object_name
		)
		return response.data.content

	def write_object(self, object_name, data, bucket_name=None):
		'''
		Write data to an object

		Args:
			object_name: Name of the object
			data: Data to write (bytes or string)
			bucket_name: Bucket name (uses default if not provided)
		'''
		bucket = bucket_name or self.default_bucket
		if not bucket:
			raise ValueError('No bucket specified and no default bucket configured')

		# Convert string to bytes if needed
		if isinstance(data, str):
			data = data.encode('utf-8')

		self.client.put_object(
			namespace_name=self.namespace,
			bucket_name=bucket,
			object_name=object_name,
			put_object_body=data,
		)

	def read_csv(self, object_name, bucket_name=None, **kwargs):
		'''
		Read CSV file from bucket into pandas DataFrame

		Args:
			object_name: Name of the CSV object
			bucket_name: Bucket name (uses default if not provided)
			**kwargs: Additional arguments passed to pd.read_csv()

		Returns:
			pandas.DataFrame
		'''
		data = self.read_object(object_name, bucket_name)
		return pd.read_csv(io.BytesIO(data), **kwargs)

	def write_csv(self, df, object_name, bucket_name=None, **kwargs):
		'''
		Write pandas DataFrame to CSV in bucket

		Args:
			df: pandas DataFrame
			object_name: Name of the CSV object
			bucket_name: Bucket name (uses default if not provided)
			**kwargs: Additional arguments passed to df.to_csv()
		'''
		# Default to not including index
		if 'index' not in kwargs:
			kwargs['index'] = False

		csv_buffer = io.StringIO()
		df.to_csv(csv_buffer, **kwargs)
		self.write_object(object_name, csv_buffer.getvalue(), bucket_name)

	def read_parquet(self, object_name, bucket_name=None, **kwargs):
		'''
		Read Parquet file from bucket into pandas DataFrame

		Args:
			object_name: Name of the Parquet object
			bucket_name: Bucket name (uses default if not provided)
			**kwargs: Additional arguments passed to pd.read_parquet()

		Returns:
			pandas.DataFrame
		'''
		data = self.read_object(object_name, bucket_name)
		return pd.read_parquet(io.BytesIO(data), **kwargs)

	def write_parquet(self, df, object_name, bucket_name=None, **kwargs):
		'''
		Write pandas DataFrame to Parquet in bucket

		Args:
			df: pandas DataFrame
			object_name: Name of the Parquet object
			bucket_name: Bucket name (uses default if not provided)
			**kwargs: Additional arguments passed to df.to_parquet()
		'''
		parquet_buffer = io.BytesIO()
		df.to_parquet(parquet_buffer, **kwargs)
		self.write_object(object_name, parquet_buffer.getvalue(), bucket_name)

	def delete_object(self, object_name, bucket_name=None):
		'''
		Delete an object from bucket

		Args:
			object_name: Name of the object
			bucket_name: Bucket name (uses default if not provided)
		'''
		bucket = bucket_name or self.default_bucket
		if not bucket:
			raise ValueError('No bucket specified and no default bucket configured')

		self.client.delete_object(
			namespace_name=self.namespace, bucket_name=bucket, object_name=object_name
		)

	def upload_file(self, file_path, object_name=None, bucket_name=None):
		'''
		Upload a file to bucket

		Args:
			file_path: Path to local file
			object_name: Name in bucket (uses filename if not provided)
			bucket_name: Bucket name (uses default if not provided)
		'''
		file_path = Path(file_path)
		if not file_path.exists():
			raise FileNotFoundError(f'File not found: {file_path}')

		if object_name is None:
			object_name = file_path.name

		# For large files, use multipart upload
		file_size = file_path.stat().st_size
		if file_size > 128 * 1024 * 1024:  # 128 MB
			self._multipart_upload(file_path, object_name, bucket_name)
		else:
			with open(file_path, 'rb') as f:
				self.write_object(object_name, f.read(), bucket_name)

	def _multipart_upload(self, file_path, object_name, bucket_name=None):
		'''Handle multipart upload for large files'''
		bucket = bucket_name or self.default_bucket
		if not bucket:
			raise ValueError('No bucket specified and no default bucket configured')

		# Use OCI's UploadManager for efficient multipart uploads
		upload_manager = object_storage.UploadManager(
			self.client, allow_parallel_uploads=True, parallel_process_count=3
		)

		with open(file_path, 'rb') as f:
			upload_manager.upload_stream(
				namespace_name=self.namespace,
				bucket_name=bucket,
				object_name=object_name,
				stream_ref=f,
				part_size=10 * 1024 * 1024,  # 10 MB parts
			)

	def download_file(self, object_name, file_path, bucket_name=None):
		'''
		Download an object to a local file

		Args:
			object_name: Name of the object
			file_path: Path to save the file
			bucket_name: Bucket name (uses default if not provided)
		'''
		data = self.read_object(object_name, bucket_name)
		file_path = Path(file_path)
		file_path.parent.mkdir(parents=True, exist_ok=True)
		with open(file_path, 'wb') as f:
			f.write(data)