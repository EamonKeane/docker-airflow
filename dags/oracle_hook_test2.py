from airflow.hooks.oracle_hook import OracleHook
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

class SQLTemplatedPythonOperator(PythonOperator):
    
    # somehow ('.sql',) doesn't work but tuple of two works...
    template_ext = ('.sql','.abcdefg')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 2),
    'email': ['eamon@logistio.ie'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id= "lifetime-template-sql",
    default_args=default_args,
    schedule_interval="@once",
    template_searchpath='/usr/local/airflow/dags/sql'
)

def get_bgf(oracle_conn_id, ds, **context):
    from lifetimes.utils import calibration_and_holdout_data
    from lifetimes.plotting import plot_frequency_recency_matrix
    from lifetimes.plotting import plot_probability_alive_matrix
    from lifetimes.plotting import plot_calibration_purchases_vs_holdout_purchases
    from lifetimes.plotting import plot_period_transactions
    from lifetimes.plotting import plot_history_alive
    from lifetimes.plotting import plot_cumulative_transactions
    from lifetimes.utils import expected_cumulative_transactions
    from lifetimes.utils import summary_data_from_transaction_data
    from lifetimes import BetaGeoFitter
    from lifetimes import GammaGammaFitter
    import datetime
    import pandas as pd
    import datalab.storage as gcs
    conn = OracleHook(oracle_conn_id=oracle_conn_id).get_conn()
    print(context)
    query = context['templates_dict']['query']
    data = pd.read_sql(query, con=conn)
    data.columns = data.columns.str.lower()

    calibration_end_date = datetime.datetime(2018,5,24)
    training_rfm = calibration_and_holdout_data(transactions=data, 
                                    customer_id_col='src_user_id', 
                                    datetime_col='pickup_date', 
                                    calibration_period_end=calibration_end_date, 
                                    freq='D', 
                                    monetary_value_col='price_total')
    bgf = BetaGeoFitter(penalizer_coef=0.0)
    bgf.fit(training_rfm['frequency_cal'], training_rfm['recency_cal'], training_rfm['T_cal'])
    print(bgf)
    
    data.to_csv('icabbi-test.csv')
    GoogleCloudStorageHook(google_cloud_storage_conn_id='google_cloud_default').upload(bucket='icabbi-data', 
            object='icabbi-test.csv', 
            filename='icabbi-test.csv')

run_this = SQLTemplatedPythonOperator(
        templates_dict={'query': 'bookings_clv.sql'},
        op_kwargs={"oracle_conn_id": "oracle_icabbi"},
        task_id='get_bgf',
        params={"client_id":"261", "bookings_start_date":"2018/05/23", "bookings_end_date":"2018/05/26"},
        python_callable=get_bgf,
        provide_context=True,
        dag=dag,
    )

run_this