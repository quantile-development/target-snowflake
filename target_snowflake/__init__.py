import singer
from singer import utils
from target_postgres import target_tools
from target_redshift.s3 import S3

from target_snowflake.connection import connect
from target_snowflake.snowflake import SnowflakeTarget

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'snowflake_account',
    'snowflake_warehouse',
    'snowflake_database',
    'snowflake_username',
    # Password is no longer strictly required if using private key auth
]


def main(config, input_stream=None):
    # Check for authentication method
    if not config.get('snowflake_password') and not config.get('snowflake_private_key_path'):
        raise Exception("Either 'snowflake_password' or 'snowflake_private_key_path' must be provided")
        
    connection_params = {
        'user': config.get('snowflake_username'),
        'role': config.get('snowflake_role'),
        'authenticator': config.get('snowflake_authenticator', 'snowflake'),
        'account': config.get('snowflake_account'),
        'warehouse': config.get('snowflake_warehouse'),
        'database': config.get('snowflake_database'),
        'schema': config.get('snowflake_schema', 'PUBLIC'),
        'autocommit': False
    }
    
    # Add authentication parameters based on provided config
    if config.get('snowflake_password'):
        connection_params['password'] = config.get('snowflake_password')
    elif config.get('snowflake_private_key_path'):
        connection_params['private_key_path'] = config.get('snowflake_private_key_path')
        if config.get('snowflake_private_key_passphrase'):
            connection_params['private_key_passphrase'] = config.get('snowflake_private_key_passphrase')

    with connect(**connection_params) as connection:
        s3_config = config.get('target_s3')

        s3 = None
        if s3_config:
            s3 = S3(s3_config.get('aws_access_key_id'),
                    s3_config.get('aws_secret_access_key'),
                    s3_config.get('bucket'),
                    s3_config.get('key_prefix'))

        target = SnowflakeTarget(
            connection,
            s3=s3,
            logging_level=config.get('logging_level'),
            persist_empty_tables=config.get('persist_empty_tables')
        )

        if input_stream:
            target_tools.stream_to_target(input_stream, target, config=config)
        else:
            target_tools.main(target)


def cli():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    main(args.config)
