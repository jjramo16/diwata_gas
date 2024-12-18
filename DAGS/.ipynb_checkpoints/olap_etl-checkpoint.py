from airflow import DAG
from datetime import datetime
from airflow.io.path import ObjectStoragePath
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import pendulum

with DAG(dag_id='olap_etl',
         schedule='@daily',
         start_date=pendulum.datetime(2024, 9, 19, tz="Asia/Manila"),
        ) as dag:
    @task
    def dim_etl():
        """Extract, transform, and load data for OLAP dimensions.
        """
        # Extract
        hook = PostgresHook(postgres_conn_id="diwata_gas_oltp")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""SELECT c.customerid,
            c.customername, c.customertype, s.salespersonname AS salespersonname
            FROM customer c JOIN salesperson s ON c.salespersonid = s.salespersonid""")
        customer_dim = cursor.fetchall()
        cursor.execute("""SELECT branchid, branchname, branchmanager FROM branch""")
        branch_dim = cursor.fetchall()
        cursor.execute("""SELECT productid, producttype, productcategory FROM product""")
        product_dim = cursor.fetchall()
        cursor.execute("""SELECT areaid, region, province, city, barangay FROM area""")
        area_dim = cursor.fetchall()
        cursor.execute("""SELECT cylindertypeid, cylindertype, cylindervolume FROM cylindertype""")
        cylindertype_dim = cursor.fetchall()
        cursor.execute("""SELECT paymentid, paymenttype, paymentdescription FROM payment""")
        payment_dim = cursor.fetchall()
        cursor.execute("""SELECT driverid, drivername, hiredate, licenseno, restrictionno FROM driver""")
        driver_dim = cursor.fetchall()

        # Transform and Load
        hook2 = RedshiftSQLHook(redshift_conn_id='diwata_gas_olap')
        conn2 = hook2.get_conn()
        cursor2 = conn2.cursor()

        cursor2.execute("""DELETE FROM customerdimension""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO customerdimension
        (customerid, customername, customertype, salespersonname)
        VALUES (%s, %s, %s, %s)""", customer_dim)
        conn2.commit()
        
        cursor2.execute("""DELETE FROM branchdimension""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO branchdimension
        (branchid, branchname, branchmanager)
        VALUES (%s, %s, %s)""", branch_dim)
        conn2.commit()

        cursor2.execute("""DELETE FROM productdimension""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO productdimension
        (productid, producttype, productcategory)
        VALUES (%s, %s, %s)""", product_dim)
        conn2.commit()

        cursor2.execute("""DELETE FROM areadimension""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO areadimension
        (areaid, region, province, city, barangay)
        VALUES (%s, %s, %s, %s, %s)""", area_dim)
        conn2.commit()

        cursor2.execute("""DELETE FROM cylindertypedimension""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO cylindertypedimension
        (cylindertypeid, cylindertype, cylindervolume)
        VALUES (%s, %s, %s)""", cylindertype_dim)
        conn2.commit()

        cursor2.execute("""DELETE FROM paymentdimension""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO paymentdimension
        (paymentid, paymenttype, paymentdescription)
        VALUES (%s, %s, %s)""", payment_dim)
        conn2.commit()

        cursor2.execute("""DELETE FROM driverdimension""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO driverdimension
        (driverid, drivername, hiredate, licenseno, restrictionno)
        VALUES (%s, %s, %s, %s, %s)""", driver_dim)
        conn2.commit()
        
        cursor2.close()
        conn2.close()
        cursor.close()
        conn.close()

    @task
    def fact_etl():
        """Extract, transform, and load data for OLAP Fact Tables.
        """
        hook = PostgresHook(postgres_conn_id="diwata_gas_oltp")
        conn = hook.get_conn()
        cursor = conn.cursor()
        hook2 = RedshiftSQLHook(redshift_conn_id='diwata_gas_olap')
        conn2 = hook2.get_conn()
        cursor2 = conn2.cursor()

        # Fact Sales
        cursor.execute("""SELECT to_char(c.orderdate, 'YYYYMMDD'), c.customerid, c.branchid,
            c.paymentid, a.areaid, cylinder.productid,
            cylinder.cylindertypeid, COUNT(cylinder.serialno) AS quantity,
            SUM(o.price) AS revenue
            FROM customerorder c
            JOIN branch b
            ON c.branchid = b.branchid
            JOIN area a
            ON a.areaid = b.areaid
            JOIN ordercylinder o
            ON c.orderid = o.orderid
            JOIN cylinder
            ON o.serialno = cylinder.serialno
            GROUP BY c.orderdate, c.customerid, c.branchid,
            c.paymentid, a.areaid, cylinder.productid, cylinder.cylindertypeid""")

        new_fact_sales = cursor.fetchall()

        cursor2.execute("""CREATE temp TABLE temporary_staging_area (LIKE salesfacttable)""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO temporary_staging_area (dateid, customerid, branchid, paymentid,
        areaid, productid, cylindertypeid, quantity, revenue)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", new_fact_sales)
        conn2.commit()
        cursor2.execute("""SELECT dateid, customerid, branchid, paymentid,
        areaid, productid, cylindertypeid, quantity, revenue FROM salesfacttable""")
        current_fact_sales = cursor2.fetchall()
        cursor2.executemany("""DELETE FROM temporary_staging_area WHERE dateid = %s AND
        customerid = %s AND branchid = %s AND paymentid = %s AND areaid = %s
        AND productid = %s AND cylindertypeid = %s AND quantity = %s AND revenue = %s""", current_fact_sales)
        conn2.commit()
        cursor2.execute("""SELECT dateid, customerid, branchid, paymentid,
        areaid, productid, cylindertypeid, quantity, revenue FROM temporary_staging_area""")
        new_fact_sales = cursor2.fetchall()
        cursor2.executemany("""INSERT INTO salesfacttable (dateid, customerid, branchid, paymentid,
        areaid, productid, cylindertypeid, quantity, revenue)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""", new_fact_sales)
        conn2.commit()
        cursor2.execute("""DROP TABLE temporary_staging_area""")
        conn2.commit()

        # Fact Inventory
        cursor.execute("""SELECT productid, cylindertypeid,
            sum(case when cylinderstatus = 'Available' then 1 else 0 end) as AvailableQuantity,
            sum(case when cylinderstatus = 'Unavailable' then 1 else 0 end) as UnavailableQuantity
            FROM cylinder
            GROUP BY productid, cylindertypeid""")

        new_fact_inventory = [] 
        for i in cursor.fetchall():
            new_fact_inventory.append((datetime.strftime(pendulum.now('Asia/Manila').date(), '%Y%m%d'),) + i)

        cursor2.execute("""CREATE temp TABLE temporary_staging_area (LIKE inventoryfacttable)""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO temporary_staging_area (dateid, productid, cylindertypeid, availablequantity,
        unavailablequantity) VALUES (%s, %s, %s, %s, %s)""", new_fact_inventory)
        conn2.commit()
        cursor2.execute("""SELECT dateid, productid, cylindertypeid, availablequantity,
        unavailablequantity FROM inventoryfacttable""")
        current_fact_inventory = cursor2.fetchall()
        cursor2.executemany("""DELETE FROM temporary_staging_area WHERE dateid = %s AND
        productid = %s AND cylindertypeid = %s AND availablequantity = %s
        AND unavailablequantity = %s""", current_fact_inventory)
        conn2.commit()
        cursor2.execute("""SELECT dateid, productid, cylindertypeid, availablequantity,
        unavailablequantity FROM temporary_staging_area""")
        new_fact_inventory = cursor2.fetchall()
        cursor2.executemany("""INSERT INTO inventoryfacttable (dateid, productid, cylindertypeid, availablequantity,
        unavailablequantity) VALUES (%s, %s, %s, %s, %s)""", new_fact_inventory)
        conn2.commit()
        cursor2.execute("""DROP TABLE temporary_staging_area""")
        conn2.commit()

        # Fact Logistics
        cursor.execute("""
        SELECT to_char(co.orderdate, 'YYYYMMdd') AS dateid, co.customerid,
            b.areaid, c.productid,
            c.cylindertypeid, l.driverid,
            sum(case when l.deliverydate = co.orderdate then 1 else 0 end) as deliveryquantity,
            sum(case when l.pickupdate = co.orderdate then 1 else 0 end) as pickupquantity
            FROM customerorder co
            JOIN ordercylinder oc ON co.orderid = oc.orderid
            JOIN cylinder c ON oc.serialno = c.serialno
            JOIN logistics l ON co.logisticsid = l.logisticsid
            JOIN branch b ON co.branchid = b.branchid
            GROUP BY dateid, co.customerid, b.areaid, c.productid, c.cylindertypeid, l.driverid""")

        new_fact_logistics = cursor.fetchall()

        cursor2.execute("""CREATE temp TABLE temporary_staging_area (LIKE logisticsfacttable)""")
        conn2.commit()
        cursor2.executemany("""INSERT INTO temporary_staging_area (dateid, customerid, areaid, productid,
        cylindertypeid, driverid, deliveryquantity, pickupquantity
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", new_fact_logistics)
        conn2.commit()
        cursor2.execute("""SELECT dateid, customerid, areaid, productid,
        cylindertypeid, driverid, driverid, deliveryquantity, pickupquantity FROM logisticsfacttable""")
        current_fact_logistics = cursor2.fetchall()
        cursor2.executemany("""DELETE FROM temporary_staging_area WHERE dateid = %s AND
        customerid = %s AND areaid = %s AND productid = %s AND cylindertypeid = %s AND (driverid = %s)
        OR (driverid IS NULL AND %s IS NULL) AND deliveryquantity = %s AND pickupquantity = %s""", current_fact_logistics)
        conn2.commit()
        cursor2.execute("""SELECT dateid, customerid, areaid, productid,
        cylindertypeid, driverid, deliveryquantity, pickupquantity FROM temporary_staging_area""")
        new_fact_logistics = cursor2.fetchall()
        cursor2.executemany("""INSERT INTO logisticsfacttable (dateid, customerid, areaid, productid,
        cylindertypeid, driverid, deliveryquantity, pickupquantity
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", new_fact_logistics)
        conn2.commit()
        cursor2.execute("""DROP TABLE temporary_staging_area""")
        conn2.commit()
        cursor2.close()
        conn2.close()
        cursor.close()
        conn.close()
    
    dim_etl()
    fact_etl()