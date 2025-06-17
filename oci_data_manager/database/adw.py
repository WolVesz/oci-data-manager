'''Autonomous Data Warehouse operations'''

import pandas as pd
import oracledb

from oci_data_manager.config import Config


class ADWClient:
	'''Client for Oracle Autonomous Data Warehouse operations'''

	def __init__(self, config_path=None):
		'''
		Initialize ADW client

		Args:
			config_path: Path to config.yaml file
		'''
		self.config = Config(config_path)
		self.adw_config = self.config.get_adw_config()

		# Connection parameters
		self.connection_string = self.adw_config['connection_string']
		self.username = self.adw_config['username']
		self.password = self.adw_config['password']

		# Pool settings
		self.pool_min = self.adw_config.get('pool_min', 1)
		self.pool_max = self.adw_config.get('pool_max', 5)
		self.pool_increment = self.adw_config.get('pool_increment', 1)

		# Initialize connection pool
		self.pool = None
		self._init_pool()

	def _init_pool(self):
		'''Initialize connection pool'''
		try:
			self.pool = oracledb.create_pool(
				user=self.username,
				password=self.password,
				dsn=self.connection_string,
				min=self.pool_min,
				max=self.pool_max,
				increment=self.pool_increment,
			)
		except Exception as e:
			raise ConnectionError(f'Failed to create connection pool: {str(e)}')

	def _get_connection(self):
		'''Get connection from pool'''
		return self.pool.acquire()

	def read_sql(self, query, params=None, **kwargs):
		'''
		Execute SQL query and return results as DataFrame

		Args:
			query: SQL query string
			params: Query parameters (dict or tuple)
			**kwargs: Additional arguments passed to pd.read_sql()

		Returns:
			pandas.DataFrame
		'''
		with self._get_connection() as conn:
			return pd.read_sql(query, conn, params=params, **kwargs)

	def execute(self, query, params=None, commit=True):
		'''
		Execute SQL statement (INSERT, UPDATE, DELETE, etc.)

		Args:
			query: SQL statement
			params: Query parameters (dict or tuple)
			commit: Whether to commit the transaction

		Returns:
			Number of rows affected
		'''
		with self._get_connection() as conn:
			with conn.cursor() as cursor:
				cursor.execute(query, params or [])
				rowcount = cursor.rowcount
				if commit:
					conn.commit()
				return rowcount

	def execute_many(self, query, data, commit=True, batch_size=1000):
		'''
		Execute SQL statement for multiple rows

		Args:
			query: SQL statement with placeholders
			data: List of tuples/dicts with values
			commit: Whether to commit the transaction
			batch_size: Number of rows per batch

		Returns:
			Total number of rows affected
		'''
		total_rows = 0
		with self._get_connection() as conn:
			with conn.cursor() as cursor:
				# Process in batches for better performance
				for i in range(0, len(data), batch_size):
					batch = data[i : i + batch_size]
					cursor.executemany(query, batch)
					total_rows += cursor.rowcount
					if commit:
						conn.commit()
		return total_rows

	def write_dataframe(
		self, df, table_name, if_exists='append', batch_size=10000, method='multi'
	):
		'''
		Write DataFrame to database table

		Args:
			df: pandas DataFrame
			table_name: Target table name
			if_exists: 'append', 'replace', or 'fail'
			batch_size: Number of rows per batch
			method: 'multi' for faster inserts

		Returns:
			Number of rows written
		'''
		# Handle table existence
		if if_exists == 'replace':
			try:
				self.execute(f'DROP TABLE {table_name}')
			except:
				pass  # Table might not exist

		# For better performance with large datasets
		if method == 'multi' and len(df) > batch_size:
			return self._bulk_insert_dataframe(df, table_name, batch_size)

		# Standard pandas to_sql for smaller datasets
		with self._get_connection() as conn:
			rows = df.to_sql(
				table_name, conn, if_exists=if_exists, index=False, method=method
			)
			conn.commit()
			return rows

	def _bulk_insert_dataframe(self, df, table_name, batch_size=10000):
		'''
		Optimized bulk insert for large DataFrames

		Args:
			df: pandas DataFrame
			table_name: Target table name
			batch_size: Number of rows per batch

		Returns:
			Number of rows inserted
		'''
		# Prepare column names and placeholders
		columns = df.columns.tolist()
		placeholders = ', '.join([f':{i+1}' for i in range(len(columns))])
		column_names = ', '.join([f'"{col}"' for col in columns])

		insert_query = f'INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})'

		# Convert DataFrame to list of tuples
		data = [tuple(row) for row in df.itertuples(index=False, name=None)]

		# Use execute_many for bulk insert
		return self.execute_many(insert_query, data, batch_size=batch_size)

	def create_table_from_dataframe(self, df, table_name, primary_key=None):
		'''
		Create table schema from DataFrame

		Args:
			df: pandas DataFrame
			table_name: Table name to create
			primary_key: Column name(s) for primary key
		'''
		# Map pandas dtypes to Oracle types
		dtype_mapping = {
			'int64': 'NUMBER(19)',
			'int32': 'NUMBER(10)',
			'int16': 'NUMBER(5)',
			'float64': 'BINARY_DOUBLE',
			'float32': 'BINARY_FLOAT',
			'object': 'VARCHAR2(4000)',
			'datetime64[ns]': 'TIMESTAMP',
			'bool': 'NUMBER(1)',
		}

		# Build CREATE TABLE statement
		columns = []
		for col_name, dtype in df.dtypes.items():
			oracle_type = dtype_mapping.get(str(dtype), 'VARCHAR2(4000)')
			columns.append(f'"{col_name}" {oracle_type}')

		# Add primary key if specified
		if primary_key:
			if isinstance(primary_key, str):
				primary_key = [primary_key]
			pk_cols = ', '.join([f'"{col}"' for col in primary_key])
			columns.append(f'PRIMARY KEY ({pk_cols})')

		create_query = f'CREATE TABLE {table_name} ({", ".join(columns)})'
		self.execute(create_query)

	def table_exists(self, table_name):
		'''
		Check if table exists

		Args:
			table_name: Table name to check

		Returns:
			bool: True if table exists
		'''
		query = '''
			SELECT COUNT(*) as cnt
			FROM user_tables
			WHERE UPPER(table_name) = UPPER(:1)
		'''
		result = self.read_sql(query, params=(table_name,))
		return result['CNT'].iloc[0] > 0

	def get_table_info(self, table_name):
		'''
		Get table schema information

		Args:
			table_name: Table name

		Returns:
			DataFrame with column information
		'''
		query = '''
			SELECT column_name, data_type, data_length, nullable
			FROM user_tab_columns
			WHERE UPPER(table_name) = UPPER(:1)
			ORDER BY column_id
		'''
		return self.read_sql(query, params=(table_name,))

	def close(self):
		'''Close connection pool'''
		if self.pool:
			self.pool.close()