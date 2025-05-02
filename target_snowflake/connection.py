import logging
import re
import time
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key

import singer
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.json_result import DictJsonResult

# Ignore DEBUG, and INFO level messages from Snowflake Connector
logger = logging.getLogger("snowflake.connector")
logger.setLevel(logging.WARNING)


class MillisLoggingCursor(SnowflakeCursor):
    def execute(self, command, **kwargs):
        timestamp = time.monotonic()

        try:
            super(MillisLoggingCursor, self).execute(command, **kwargs)
        finally:
            self.connection.LOGGER.info(
            "MillisLoggingCursor: {} millis spent executing: {}".format(
                int((time.monotonic() - timestamp) * 1000),
                re.sub(r'\n', '  \\\\n  ', command)
            ))

        return self


class MillisLoggingDictCursor(MillisLoggingCursor):
    def __init__(self, connection):
        MillisLoggingCursor.__init__(self, connection, DictJsonResult)


class Connection(SnowflakeConnection):
    def __init__(self, **kwargs):
        self.LOGGER = singer.get_logger()

        self.configured_warehouse = kwargs.get('warehouse')
        self.configured_database = kwargs.get('database')
        self.configured_schema = kwargs.get('schema')
        
        # Handle private_key_path if provided
        private_key_path = kwargs.pop('private_key_path', None)
        if private_key_path:
            if not os.path.exists(private_key_path):
                raise FileNotFoundError(f"Private key file not found at: {private_key_path}")
            
            with open(private_key_path, 'rb') as key_file:
                p_key = key_file.read()
                
            private_key_passphrase = kwargs.pop('private_key_passphrase', None)
            kwargs['private_key'] = load_private_key(p_key, private_key_passphrase)

        SnowflakeConnection.__init__(self, **kwargs)

    def cursor(self, as_dict=False):
        cursor_class = MillisLoggingCursor
        if as_dict:
            cursor_class = MillisLoggingDictCursor

        return SnowflakeConnection.cursor(self, cursor_class)

    def initialize(self, logger):
        self.LOGGER = logger


def load_private_key(p_key, passphrase=None):
    """
    Load a private key from a PEM string with an optional passphrase.
    Returns a private key object that can be used with the Snowflake connector.
    """
    if passphrase:
        passphrase = passphrase.encode()
    
    p_key_object = load_pem_private_key(
        p_key,
        password=passphrase,
        backend=default_backend()
    )
    
    return p_key_object


def connect(**kwargs):
    return Connection(**kwargs)
