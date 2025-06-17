'''Helper utilities for OCI Data Manager'''

import pandas as pd


def validate_dataframe(df):
	'''
	Validate DataFrame for database operations

	Args:
		df: pandas DataFrame

	Returns:
		DataFrame with cleaned column names

	Raises:
		ValueError: If DataFrame is empty or invalid
	'''
	if df.empty:
		raise ValueError('DataFrame is empty')

	# Clean column names for Oracle compatibility
	df = df.copy()
	df.columns = [col.upper().replace(' ', '_') for col in df.columns]

	# Check for duplicate column names
	if len(df.columns) != len(set(df.columns)):
		raise ValueError('DataFrame contains duplicate column names')

	return df


def chunk_dataframe(df, chunk_size=10000):
	'''
	Split DataFrame into chunks for batch processing

	Args:
		df: pandas DataFrame
		chunk_size: Number of rows per chunk

	Yields:
		DataFrame chunks
	'''
	for start in range(0, len(df), chunk_size):
		yield df.iloc[start : start + chunk_size]