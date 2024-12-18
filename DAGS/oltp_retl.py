from airflow import DAG
from datetime import datetime
from airflow.io.path import ObjectStoragePath
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

with DAG(dag_id='oltp_retl',
         schedule='@monthly',
         start_date=pendulum.datetime(2024, 9, 19, tz="Asia/Manila"),
        ) as dag:
    import json
    import pandas as pd

    @task
    def oltp_retl():
        """Reverse extract, transform, and load data for OLTP and data scientists,
        and store in S3 bucket (Work Zone).
        """
        # Customer Order
        base = ObjectStoragePath("s3://s3_diwata_gas@diwata-gas-bucket/work/")
        
        hook = PostgresHook(postgres_conn_id="diwata_gas_oltp")
        conn = hook.get_conn()
        cursor = conn.cursor()
        date_today = pendulum.now("Asia/Manila").subtract(months=1).strftime("%B") + '_' + pendulum.now("Asia/Manila").strftime("%Y")
        first_day = pendulum.now("Asia/Manila").subtract(months=1).start_of('month')
        last_day = pendulum.now("Asia/Manila").subtract(months=1).end_of('month')
    
        cursor.execute("""SELECT co.orderid, co.logisticsid, co.orderdate, oc.serialno, oc.price,
                        c.productid, c.cylinderstatus, p.producttype,
                        p.productcategory, cu.customerstatus, cu.customerid,
                        a.region, a.province, a.city, a.barangay,
                        b.branchname
                        FROM CustomerOrder co
                        JOIN branch b ON b.branchid = co.branchid
                        JOIN customer cu ON cu.customerid = co.customerid
                        JOIN area a ON cu.areaid = a.areaid
                        JOIN ordercylinder oc ON co.orderid = co.orderid
                        JOIN cylinder c ON oc.serialno = c.serialno
                        JOIN product p ON c.productid = p.productid
                        WHERE orderdate >= %s AND orderdate <= %s""",
                      (first_day, last_day))
        lst_desc = [desc[0] for desc in cursor.description]
        tuple_order = cursor.fetchall()
        df = pd.DataFrame(tuple_order, columns=lst_desc)
    
        path = base / f"CustomerOrder_{date_today}.parquet"
    
        with path.open("wb") as file:
            df.to_parquet(file)
    
        # Logistics
        cursor.execute("""
        SELECT logisticsid, Logistics.driverid, deliverydate, pickupdate,
            restrictionno
            FROM Logistics
            JOIN driver ON Logistics.driverid = driver.driverid
            WHERE deliverydate >= %s AND deliverydate <= %s""",
                  (first_day, last_day))
        lst_desc = [desc[0] for desc in cursor.description]
        tuple_logistics = cursor.fetchall()
        df = pd.DataFrame(tuple_logistics, columns=lst_desc)
    
        path = base / f"Logistics_{date_today}.parquet"
    
        with path.open("wb") as file:
            df.to_parquet(file)

    oltp_retl()