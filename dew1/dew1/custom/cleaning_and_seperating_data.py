
import pandas as pd
from pandas import DataFrame
import time
from datetime import datetime, timedelta
import pytz
import uuid

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(df: DataFrame, **kwargs):
    """ 
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    
    name
    data type
    none
    duplicate
    """

    # Cleaning data
    df['date_of_birth']= df['date_of_birth'].astype('datetime64[ns]')
    df = df.drop_duplicates()
    if 'message' in df.columns:
        df = df.drop('message', axis=1)
    df = df.dropna(subset=['id','uid'])

    # Separating data
    
    # dim table
    dim_user_info = df[['id','uid','password','first_name','last_name','username','email','avatar','gender','phone_number','social_insurance_number','date_of_birth','credit_card_cc_number']]
    dim_employment = df[['employment_title','employment_key_skill']]
    dim_address = df[['address_city', 'address_street_name', 'address_street_address', 'address_zip_code', 'address_state', 'address_country', 'address_coordinates_lat', 'address_coordinates_lng']]
    dim_subscription = df[['subscription_plan', 'subscription_status', 'subscription_payment_method', 'subscription_term']]
    
    # Function to add the id column.
    def add_uuid(df: pd.DataFrame) -> pd.DataFrame:
        df['row_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        df['datetime_day'] = datetime.now().date() 
        return df

    dim_user_info = add_uuid(dim_user_info)
    dim_employment = add_uuid(dim_employment)
    dim_address = add_uuid(dim_address)
    dim_subscription = add_uuid(dim_subscription)

    # fact table

    fact_df = pd.DataFrame()
    fact_df['base_use_info'] = dim_user_info['row_id']
    fact_df['employment_info'] = dim_employment['row_id']
    fact_df['address_info'] = dim_address['row_id']
    fact_df['subscription_info'] = dim_subscription['row_id']
    fact_df = add_uuid(fact_df)


    wh_table = {
        'dim_user_info': dim_user_info,
        'dim_employment': dim_employment,
        'dim_address': dim_address,
        'dim_subscription': dim_subscription,
        'fact_table': fact_df
    }

    print(fact_df)

    return wh_table


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

