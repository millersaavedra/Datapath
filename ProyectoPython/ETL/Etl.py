# 1 Cargamos librerias

import glob  
import pandas as pd  
import xml.etree.ElementTree as ET  
from datetime import datetime
import sqlalchemy as db
from sqlalchemy import create_engine
import pymysql
import mysql.connector


import warnings
warnings.filterwarnings("ignore")

# 2 variable logs

logfile = "dealership_logfile.txt" 

# 3 Funciones para extraccion de datos

def customer():
    return ['customer_id','customer_fname','customer_lname','customer_email','customer_password','customer_street','customer_city','customer_state','customer_zipcode'],'customer','df_customer_ext'
def departments():
    return ['department_id','department_name'],'departments','df_departments_ext'
def categories():
    return ['category_id','category_department_id','category_name'],'categories','df_categories_ext'
def products():
    return ['product_id','product_category_id','product_name','product_description','product_price','product_image'],'products','df_products_ext'
def orders():
    return ['order_id','order_date','order_customer_id','order_status'],'orders','df_orders_ext'
def order_items():
    return ['order_item_id','order_item_order_id','order_item_product_id','order_item_quantity','order_item_subtotal','order_item_product_price'],'order_items','df_order_items_ext'

def headerTable(number):
    numberTable = {
        1: customer,
        2: departments,
        3: categories,
        4: products,
        5: orders,
        6: order_items,
        7: lambda: "only 1 to 6"
    }

    return numberTable.get(number, lambda: "no number table exist")()

def extract_from_csv(file_to_process,numberTable): 
    
    dataframe = pd.read_csv(file_to_process, encoding='ISO-8859-1',sep='|',header=None,names=headerTable(numberTable)[0]) 
    return dataframe

path = 'data/'
def extract(numberTable):
    extracted_data = pd.DataFrame(columns=headerTable(numberTable)[0])
    #for csv files
    for csvfile in glob.glob(path+headerTable(numberTable)[1]):
        extracted_data = extracted_data.append(extract_from_csv(csvfile,numberTable), ignore_index=True)
    return extracted_data

# 4 conexiones db

SERVER = '192.168.1.7'
DATABASE = 'DomVentas'
DRIVER = 'SQL Server Native Client 11.0'
USERNAME = 'sa'
PASSWORD = '38263416Ms$*'
DATABASE_CONNECTION = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER}'
enginesqlserver = db.create_engine(DATABASE_CONNECTION)
connsqlserver = enginesqlserver.connect()

connmysql = mysql.connector.connect(
  host="192.168.1.7",
  user="administrator",
  passwd="1971",
  database="domventas"
)
enginemysql = db.create_engine('mysql://administrator:1971@192.168.1.7:3306/domventas')
cursormysql = connmysql.cursor()

# 5funciones para carga de datos en dbs

sql = 'truncate table '

def loadsql(numberTable):
    dtf = extract(numberTable) 
    connsqlserver.execute(sql + headerTable(numberTable)[1])
    dtf.to_sql(headerTable(numberTable)[1], connsqlserver, if_exists='append', index = False)
    dtf.drop

# 6 funciones para manejo de logs

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f: f.write(timestamp + ',' + message + 'n')

# --------------------------------------------------------------------------------------------
# 7 extracción de datos de csv a sql server
# --------------------------------------------------------------------------------------------

log("ETL Job Extration Started")

#############################
log("Extract phase Started")

for num in range(1,7):      
    loadsql(num)

log("Extract phase Ended")

# --------------------------------------------------------------------------------------------
# 8 lectura de datos desde sql server a pandas df
# --------------------------------------------------------------------------------------------

log("ETL Job Read Started")

#############################
log("Read phase Started")

df_customers_ext = pd.read_sql('select * from customer', con=connsqlserver)
df_departments_ext = pd.read_sql('select * from departments', con=connsqlserver)
df_categories_ext = pd.read_sql('select * from categories', con=connsqlserver)
df_products_ext = pd.read_sql('select * from products', con=connsqlserver)
df_orders_ext = pd.read_sql('select * from orders', con=connsqlserver)
df_order_items_ext = pd.read_sql('select * from order_items', con=connsqlserver)

log("Read phase Ended")

# --------------------------------------------------------------------------------------------
# 9 cargar el subdominio productos
# --------------------------------------------------------------------------------------------

df_bvp_products = df_departments_ext.merge(df_categories_ext, left_on='department_id', right_on='category_department_id', how='outer')
df_bvp_products = df_bvp_products.merge(df_products_ext, left_on='category_id', right_on='product_category_id', how='right')

# --------------------------------------------------------------------------------------------
# 10 cargar el subdominio clientes
# --------------------------------------------------------------------------------------------

df_bvc_customer = df_customers_ext

# --------------------------------------------------------------------------------------------
# 11 cargar el subdominio ventas
# --------------------------------------------------------------------------------------------

df_desc_orders_ext = df_order_items_ext.merge(df_orders_ext, left_on='order_item_order_id', right_on='order_id', how='right', indicator=True) \
  .query('_merge == "right_only"') \
  .drop('_merge', 1)[['order_id','order_date','order_customer_id','order_status']] 

#eliminando las ordenes que no tienen un order_item_order_id asignado
df_orders_ext_new = df_desc_orders_ext.merge(df_orders_ext, left_on='order_id', right_on='order_id', how='right', indicator=True) \
  .query('_merge == "right_only"') \
  .drop('_merge', 1)[['order_id','order_date_y','order_customer_id_y','order_status_y']] 

df_orders_ext_new.columns = ['order_id', 'order_date', 'order_customer_id', 'order_status']

#Eliminar la columna order_item_id es corrupta
df_order_items_ext = df_order_items_ext.drop(['order_item_id'], axis=1)

df_bvo_orders = df_orders_ext_new.merge(df_order_items_ext, left_on='order_id', right_on='order_item_order_id', how='right')

# --------------------------------------------------------------------------------------------
# 12 cargar los subdominios en el market de datos mysql
# --------------------------------------------------------------------------------------------

log("ETL Job Extration Started")

#############################
log("Load subdomain phase Started")

cursormysql.execute('truncate table bvp_products')
cursormysql.execute('truncate table bvc_customer')
cursormysql.execute('truncate table bvo_orders')

df_bvp_products.to_sql('bvp_products', enginemysql, if_exists='append', index=False)    
df_bvc_customer.to_sql('bvc_customer', enginemysql, if_exists='append', index=False)
df_bvo_orders.to_sql('bvo_orders', enginemysql, if_exists='append', index=False)

df_bvp_products.drop
df_bvc_customer.drop
df_bvo_orders.drop

log("Load subdomain phase Ended")
