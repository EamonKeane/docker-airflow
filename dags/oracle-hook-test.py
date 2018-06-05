from airflow.hooks.oracle_hook import OracleHook
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 1),
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
    dag_id='lifetime_python_operator20', default_args=default_args,
    schedule_interval=None)

def get_bgf(oracle_conn_id):
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

    query = """WITH BOOKINGS AS(
    SELECT B.SRC_BOOKING_ID,
        B.CLIENT_ID,
        T.TIME_VALUE AS PICKUP_TIME,
        D.DATE_DT AS PICKUP_DATE,
        U.SRC_USER_ID
        FROM DWH.WC_BOOKING_F B,
        DWH.WC_DATE_D D,
        DWH.WC_TIME_D T,
        DWH.WC_USER_D U
    WHERE B.CLIENT_ID=261
    and B.PICKUP_DATE_ID=D.DATE_ID
    AND B.PICKUP_TIME_ID=T.TIME_ID
    and B.USER_ID=U.USER_ID
    ),
    PAYMENTS AS(
    SELECT SRC_BOOKING_ID, PRICE_TOTAL
    FROM DWH.WC_PAYMENT_F
    WHERE CLIENT_ID=261
    )
    select DISTINCT BOOKINGS.SRC_USER_ID,
                    BOOKINGS.PICKUP_DATE,
                    BOOKINGS.PICKUP_TIME,
                    PAYMENTS.PRICE_TOTAL,
                    BOOKINGS.SRC_BOOKING_ID
    from BOOKINGS
    INNER JOIN PAYMENTS
    on BOOKINGS.SRC_BOOKING_ID=PAYMENTS.SRC_BOOKING_ID
        where PAYMENTS.PRICE_TOTAL > 0
        and PICKUP_DATE between TO_DATE ('2018/05/23', 'yyyy/mm/dd')
        AND TO_DATE ('2018/05/26', 'yyyy/mm/dd')"""

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

run_this = PythonOperator(
    task_id='get_bgf',
    provide_context=False,
    python_callable=get_bgf,
    op_kwargs={'oracle_conn_id': 'oracle_icabbi'},
    dag=dag)

#gcs.Bucket('airflow-test-data-icabbi').item('to/data.csv').write_to(data.to_csv(),'text/csv')

