from datetime import datetime
from lifetimes import BetaGeoFitter
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from airflow import DAG

from airflow.operators.python_operator import PythonOperator

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
    dag_id='lifetime_python_operator10', default_args=default_args,
    schedule_interval=None)

def lifetime_clv():
    oracle_connection_string = (
    'oracle+cx_oracle://{username}:{password}@' +
    cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}')
    )

    engine = create_engine(
        oracle_connection_string.format(
            username='icabbiuk_owner',
            password='DiSC#B00K',
            hostname='141.144.120.94',
            port='1521',
            service_name='discbookdw.icabbiukcloud.oraclecloud.internal',
        )
    )
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
        and PICKUP_DATE between TO_DATE ('2018/05/10', 'yyyy/mm/dd')
        AND TO_DATE ('2018/05/27', 'yyyy/mm/dd')"""

    data = pd.read_sql(query, engine)

    calibration_end_date = datetime.datetime(2018,5,22)
    training_rfm = calibration_and_holdout_data(transactions=data, 
                                   customer_id_col='src_user_id', 
                                   datetime_col='pickup_date', 
                                   calibration_period_end=calibration_end_date, 
                                   freq='D', 
                                   monetary_value_col='price_total')
    bgf = BetaGeoFitter(penalizer_coef=0.0)
    bgf.fit(training_rfm['frequency_cal'], training_rfm['recency_cal'], training_rfm['T_cal'])
    print(bgf)

run_this = PythonOperator(
    task_id='calculate_bgf',
    provide_context=False,
    python_callable=lifetime_clv,
    dag=dag)
    