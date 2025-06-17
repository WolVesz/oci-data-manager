
'''OCI Data Manager - Simple wrapper for Oracle Cloud Infrastructure SDK'''

from oci_data_manager.database.adw import ADWClient
from oci_data_manager.storage.bucket import StorageClient

__version__ = '0.1.0'
__all__ = ['StorageClient', 'ADWClient']