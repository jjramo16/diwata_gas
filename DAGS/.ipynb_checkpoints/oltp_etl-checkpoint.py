from airflow import DAG
from datetime import datetime
from airflow.io.path import ObjectStoragePath
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

with DAG(dag_id='oltp_etl',
         schedule='@daily',
         start_date=pendulum.datetime(2024, 9, 19, tz="Asia/Manila"),
        ) as dag:
    import json
    import pandas as pd
    
    base = ObjectStoragePath("s3://s3_diwata_gas@diwata-gas-bucket/")
    
    @task
    def customer_extract():
        """Extract customer file paths from S3 bucket.

        Returns
        -------
        customer_files : list
            List of customer file paths
        """
        customer_base = base / 'sensitive' / 'Customer'
        customer_files = [f for f in customer_base.iterdir() if f.is_file()]
        return customer_files

    @task
    def log_extract():
        """Extract logistics file paths from S3 bucket.

        Returns
        -------
        log_files : list
            List of logistics file paths
        """
        log_base = base / 'landing' / 'Logistics'
        log_files = [f for f in log_base.iterdir() if f.is_file()]
        return log_files

    @task
    def order_extract():
        """Extract order file paths from S3 bucket.

        Returns
        -------
        order_files : list
            List of order file paths
        """
        order_base = base / 'landing' / 'Orders'
        order_files = [f for f in order_base.iterdir() if f.is_file()]
        return order_files

    @task
    def customer_load(customer_files):
        """Load or upsert customer data into OLTP.
        """
        hook = PostgresHook(postgres_conn_id="diwata_gas_oltp")
        conn = hook.get_conn()
        cursor = conn.cursor()
        lst_rows = []
        for filepath in customer_files:
            customer_json = json.loads(filepath.read_text())
            cursor.execute("""SELECT areaid FROM area WHERE region = %s
            AND province = %s AND city = %s AND barangay = %s""", (customer_json['AreaRegion'],
                                                         customer_json['AreaProvince'],
                                                         customer_json['AreaCity'],
                                                         customer_json['AreaBarangay']))
            area_id = cursor.fetchone()[0]
            cursor.execute("""
                SELECT salespersonid FROM salesperson WHERE salespersonname = %s""",
                (customer_json['SalesPersonName'],))
            salesperson_id = cursor.fetchone()[0]
            if area_id and salesperson_id:
                lst_rows.append((customer_json['CustomerName'],
                                 customer_json['CustomerType'],
                                 customer_json['CustomerAddress'],
                                 area_id,
                                 customer_json['ContactNumber'],
                                 'Active',
                                 salesperson_id))
        cursor.executemany("""
            INSERT INTO customer (customername, customertype, customeraddress, areaid, contactnumber, customerstatus, salespersonid)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customername)
            DO NOTHING""", lst_rows)
        conn.commit()
        cursor.close()
        conn.close()

    @task
    def doc_load(order_files):
        """Load or upsert document reference data for DynamoDB
        """
        hook = PostgresHook(postgres_conn_id="diwata_gas_oltp")
        conn = hook.get_conn()
        cursor = conn.cursor()
        for filepath in order_files:
            order_json = json.loads(filepath.read_text())
            cursor.execute("""
                    INSERT INTO documentreference (documentreferencenumber, documentreferencetype)
                    VALUES (%s, %s)
                    ON CONFLICT (documentreferencenumber)
                    DO NOTHING""", (order_json['DocumentReferenceNumber'],
                                   order_json['DocumentReferenceType']))
            conn.commit()
        cursor.close()
        conn.close()
    
    @task
    def order_load(order_files):
        """Load or upsert order data into OLTP.
        """
        hook = PostgresHook(postgres_conn_id="diwata_gas_oltp")
        conn = hook.get_conn()
        cursor = conn.cursor()
        for filepath in order_files:
            try:
                order_json = json.loads(filepath.read_text())
                cursor.execute("""SELECT branchid FROM branch WHERE branchname = %s""", (order_json['BranchName'],))
                branch_id = cursor.fetchone()[0]
                cursor.execute("""SELECT customerid FROM customer WHERE customername = %s""", (order_json['OrderName'],))
                customer_id = cursor.fetchone()[0]
                cursor.execute("""SELECT paymentid FROM payment WHERE paymenttype = %s""", (order_json['PaymentType'],))
                payment_id = cursor.fetchone()[0]
                cursor.execute("""SELECT MAX(logisticsid) FROM logistics""")
                logistics_id = cursor.fetchone()[0]
                if logistics_id is None:
                    logistics_id = 0
                cursor.execute("""SELECT MAX(orderid) FROM customerorder""")
                old_order_id = cursor.fetchone()[0]
                cursor.execute("""
                INSERT INTO customerorder (branchid, customerid, documentreferencenumber, orderdate, logisticsid, paymentid)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (branchid, customerid, documentreferencenumber, orderdate, paymentid)
                DO NOTHING""", (branch_id,
                               customer_id,
                               order_json['DocumentReferenceNumber'],
                               order_json['OrderDate'],
                               logistics_id + 1,
                               payment_id))
                conn.commit()
                cursor.execute("""SELECT MAX(orderid) FROM customerorder""")
                new_order_id = cursor.fetchone()[0]
                if old_order_id != new_order_id:
                    lst_rows_ordercylinder = []
                    cursor.execute("""
                        INSERT INTO logistics (driverid, deliverydate, pickupdate)
                        VALUES (%s, %s, %s)""", (None, None, None))
                    conn.commit()
                    for i, j in order_json['Prices'].items():
                        cursor.execute("""
                        INSERT INTO ordercylinder (orderid, serialno, price)
                        VALUES (%s, %s, %s)""", (new_order_id, i, j))
                        conn.commit()
            except Exception:
                continue
        cursor.close()
        conn.close()

    @task
    def log_load(log_files):
        """Load or upsert logistics data into OLTP.
        """
        hook = PostgresHook(postgres_conn_id="diwata_gas_oltp")
        conn = hook.get_conn()
        cursor = conn.cursor()
        lst_rows = []
        for filepath in log_files:
            log_json = json.loads(filepath.read_text())
            cursor.execute("""SELECT driverid FROM driver WHERE drivername = %s""", (log_json['DriverName'],))
            driver_id = cursor.fetchone()[0]
            if driver_id:
                if log_json['PickUpDate'] == "":
                    log_json['PickUpDate'] = None
                lst_rows.append((driver_id, log_json['DeliveryDate'], log_json['PickUpDate'], log_json['LogisticsID']))
        cursor.executemany("""
            UPDATE logistics
            SET driverid = %s, deliverydate = %s, pickupdate = %s
            WHERE logisticsid = %s;""", lst_rows)
        conn.commit()
        cursor.execute("""
            UPDATE cylinder
            SET cylinderstatus = 'Available'
            FROM ordercylinder oc
            JOIN customerorder co
            ON oc.orderid = co.orderid
            JOIN logistics l
            ON co.logisticsid = l.logisticsid
            WHERE cylinder.serialno = oc.serialno
            AND l.deliverydate IS NOT NULL
            AND l.pickupdate IS NOT NULL""")
        conn.commit()
        cursor.execute("""
            UPDATE cylinder
            SET cylinderstatus = 'Unavailable'
            FROM ordercylinder oc
            JOIN customerorder co
            ON oc.orderid = co.orderid
            JOIN logistics l
            ON co.logisticsid = l.logisticsid
            WHERE cylinder.serialno = oc.serialno
            AND l.deliverydate IS NOT NULL
            AND l.pickupdate IS NULL""")
        conn.commit()
        cursor.close()
        conn.close()

    customer_files = customer_extract()
    order_files = order_extract()
    log_files = log_extract()
    customer_load(customer_files) >> doc_load(order_files) >> order_load(order_files) >> log_load(log_files)