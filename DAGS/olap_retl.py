from airflow import DAG
from datetime import datetime
from airflow.io.path import ObjectStoragePath
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import pendulum

with DAG(dag_id='olap_retl',
         schedule='@monthly',
         start_date=pendulum.datetime(2024, 9, 19, tz="Asia/Manila"),
        ) as dag:
    
    @task
    def olap_retl():
        """Reverse extract, transform, and load data for OLAP and data analysts, and store
        in S3 bucket (Gold Zone).
        """
        import pandas as pd
        
        # Sales Fact Table
        base = ObjectStoragePath("s3://s3_diwata_gas@diwata-gas-bucket/gold/")
        
        hook = RedshiftSQLHook(redshift_conn_id='diwata_gas_olap')
        conn = hook.get_conn()
        cursor = conn.cursor()
        date_today = pendulum.now("Asia/Manila").subtract(months=1).strftime("%B") + '_' + pendulum.now("Asia/Manila").strftime("%Y")
        first_day = pendulum.now("Asia/Manila").subtract(months=1).start_of('month')
        last_day = pendulum.now("Asia/Manila").subtract(months=1).end_of('month')
    
        cursor.execute("""SELECT d.dateid, customerid, branchid, paymentid,
        areaid, productid, cylindertypeid, quantity, revenue FROM salesfacttable s JOIN datedimension d
        ON s.dateid = d.dateid
        WHERE date >= %s AND date <= %s""",
                      (first_day, last_day))
        try:
            tuple_sales = cursor.fetchall()
            df = pd.DataFrame(tuple_sales, columns=['dateid', 'customerid', 'branchid', 'paymentid',
            'areaid', 'productid', 'cylindertypeid', 'quantity', 'revenue'])
        
            path = base / f"SalesFactTable_{date_today}.csv"
        
            with path.open("wb") as file:
                df.to_csv(file)
        except Exception:
            pass
    
        # Inventory Fact Table
        cursor.execute("""SELECT d.dateid, productid, cylindertypeid, availablequantity, unavailablequantity 
        FROM inventoryfacttable i JOIN datedimension d
        ON i.dateid = d.dateid
        WHERE date >= %s AND date <= %s""",
                  (first_day, last_day))
        try:
            tuple_inventory = cursor.fetchall()
            df = pd.DataFrame(tuple_inventory, columns=['dateid', 'productid', 'cylindertypeid', 'availablequantity',
            'unavailablequantity'])
        
            path = base / f"InventoryFactTable_{date_today}.csv"
        
            with path.open("wb") as file:
                df.to_csv(file)
        except Exception:
            pass

        # Logistics Fact Table
        cursor.execute("""SELECT d.dateid, customerid, areaid, productid,
        cylindertypeid, driverid, deliveryquantity, pickupquantity FROM logisticsfacttable l JOIN datedimension d
        ON l.dateid = d.dateid
        WHERE date >= %s AND date <= %s""", (first_day, last_day))
        try:
            tuple_logistics = cursor.fetchall()
            df = pd.DataFrame(tuple_logistics, columns=['dateid', 'customerid', 'areaid', 'productid',
            'cylindertypeid', 'driverid', 'deliveryquantity', 'pickupquantity'])
            
            path = base / f"LogisticsFactTable_{date_today}.csv"
    
            with path.open("wb") as file:
                df.to_csv(file)

        except Exception:
            pass
    
    olap_retl()