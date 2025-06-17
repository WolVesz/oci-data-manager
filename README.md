# OCI Data Manager

A simple Python wrapper around Oracle Cloud Infrastructure (OCI) SDK focused on Object Storage and Autonomous Data Warehouse operations with pandas integration.

## Features

- **Object Storage Operations**
  - Read/write objects (files, CSVs, Parquet)
  - List and delete objects
  - Bulk upload with automatic multipart handling
  - Direct pandas DataFrame integration

- **Autonomous Data Warehouse Operations**
  - Execute queries with pandas DataFrame results
  - Bulk insert DataFrames with optimized performance
  - Connection pooling for efficiency
  - Table creation and schema inspection

## Installation

```bash
pip install oci-data-manager
```

Or install from source:

```bash
git clone https://github.com/yourusername/oci-data-manager.git
cd oci-data-manager
pip install -e .
```

## Configuration

1. Copy `config.yaml.example` to `config.yaml`:
```bash
cp config.yaml.example config.yaml
```

2. Edit `config.yaml` with your OCI credentials:
```yaml
oci:
  user: ocid1.user.oc1..aaaaaaaaaaaa...
  key_file: ~/.oci/oci_api_key.pem
  fingerprint: aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99
  tenancy: ocid1.tenancy.oc1..aaaaaaaaaaaa...
  region: us-ashburn-1

storage:
  namespace: your-namespace
  default_bucket: your-default-bucket

adw:
  connection_string: (description=(retry_count=20)...)
  username: your-username
  password: your-password
```

The config file can be placed in:
- Current directory: `./config.yaml`
- Home directory: `~/config.yaml`
- Custom path via environment variable: `OCI_DATA_MANAGER_CONFIG=/path/to/config.yaml`

## Quick Start

### Object Storage

```python
from oci_data_manager import StorageClient
import pandas as pd

# Initialize client
storage = StorageClient()

# List objects
objects = storage.list_objects(prefix='data/')

# Read CSV from bucket
df = storage.read_csv('data/sales.csv')

# Write DataFrame to bucket
results_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
storage.write_csv(results_df, 'results/output.csv')

# Upload local file
storage.upload_file('large_file.zip', 'backups/large_file.zip')

# Read Parquet file
df = storage.read_parquet('data/large_dataset.parquet')
```

### Autonomous Data Warehouse

```python
from oci_data_manager import ADWClient
import pandas as pd

# Initialize client
adw = ADWClient()

# Query data
df = adw.read_sql("SELECT * FROM sales WHERE region = :region", 
                  params={'region': 'WEST'})

# Write DataFrame to table
sales_df = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=100),
    'amount': np.random.randn(100) * 1000,
    'region': ['WEST'] * 50 + ['EAST'] * 50
})

# Write with automatic table creation
adw.write_dataframe(sales_df, 'sales_data', if_exists='replace')

# Bulk insert for performance
rows_inserted = adw.write_dataframe(
    large_df, 
    'large_table',
    batch_size=50000,
    method='multi'
)

# Execute DDL/DML
adw.execute("CREATE INDEX idx_region ON sales_data(region)")

# Check if table exists
if adw.table_exists('sales_data'):
    info = adw.get_table_info('sales_data')
    print(info)
```

## Advanced Usage

### Custom Config Path

```python
storage = StorageClient(config_path='/opt/app/config.yaml')
adw = ADWClient(config_path='/opt/app/config.yaml')
```

### Handling Large Files

The storage client automatically uses multipart upload for files larger than 128MB:

```python
# Automatic multipart upload for large files
storage.upload_file('data/huge_dataset.parquet')
```

### Batch Processing

```python
from oci_data_manager.utils import chunk_dataframe

# Process large DataFrame in chunks
for chunk in chunk_dataframe(huge_df, chunk_size=50000):
    adw.write_dataframe(chunk, 'target_table', if_exists='append')
```

### Connection Pool Management

```python
# Close connection pool when done
adw.close()
```

## Development

### Setup Development Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .
pip install ruff
```

### Code Formatting and Linting

```bash
# Format code
ruff format .

# Check linting
ruff check .
```

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests
pytest tests/
```

## Example Code

```python 
# Storage operations
storage = StorageClient()
df = storage.read_csv('data/file.csv')
storage.write_csv(df, 'output/results.csv')

# ADW operations
adw = ADWClient()
results = adw.read_sql("SELECT * FROM table")
adw.write_dataframe(df, 'new_table', batch_size=50000)
```

## Best Practices

1. **Configuration Security**: Never commit `config.yaml` with real credentials
2. **Connection Management**: Use context managers or ensure proper cleanup
3. **Batch Operations**: Use batch_size parameter for large data operations
4. **Error Handling**: The wrapper passes through OCI SDK exceptions for detailed error information

## Limitations

- Supports only pandas DataFrames for data operations
- Requires Oracle Instant Client for ADW connections
- Limited to basic read/write operations (no advanced OCI features)

## License

MIT License - see LICENSE file for details

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request