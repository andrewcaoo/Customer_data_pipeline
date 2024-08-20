from pyiceberg.catalog.hive import HiveCatalog
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(wh_table:dict, *args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here
        

    hive_catalog = HiveCatalog(
        "default",
        **{
            "uri": "thrift://hive-metastore:9083",  # Hive Metastore URI
            "warehouse": "http://minio:9000/",  # Path to Iceberg warehouse on MinIO
            "fs.s3a.access.key": "minio",  # MinIO access key
            "fs.s3a.secret.key": "minio123",  # MinIO secret key
            "fs.s3a.endpoint": "http://minio:9000",  # MinIO endpoint URL
            "fs.s3a.path.style.access": "true"  # MinIO path style access
        },
    )

    hive_catalog.create_namespace("lhdw1")
    print(hive_catalog.list_namespaces())
    print(hive_catalog.list_tables('default'))
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
