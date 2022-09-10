import pymysql

class Database:
    def connect(self):
        return pymysql.connect(host="192.168.1.7", user="administrator", password="1971",database="classicmodels",charset="latin1")
    
    def readVenDep(self):
        con = self.connect()
        cursor = con.cursor(pymysql.cursors.DictCursor)
        sql = "SELECT  a.department_name  ,sum(b.order_item_subtotal) FROM domventas.bvp_products a inner join domventas.bvo_orders b on a.product_id = b.order_item_product_id where b.order_status='COMPLETE' group by a.department_name"
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            return e
        finally:
            con.close

    def readcatCom(self):
        con = self.connect()
        cursor = con.cursor(pymysql.cursors.DictCursor)
        sql = "SELECT  a.category_name  ,count(distinct b.order_customer_id)cantidad_compras FROM domventas.bvp_products a inner join domventas.bvo_orders b on a.product_id = b.order_item_product_id where b.order_status='COMPLETE' group by a.category_name order by cantidad_compras desc"
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            return e
        finally:
            con.close

    def readcliCom(self,top):
        con = self.connect()
        cursor = con.cursor(pymysql.cursors.DictCursor)
        sql = f"select concat(b.customer_fname,' ',b.customer_lname) nombres ,count(b.customer_id) cantidad_compras ,ROW_NUMBER() OVER ( ORDER BY count(b.customer_id) desc) posicion from domventas.bvo_orders a inner join domventas.bvc_customer b on a.order_customer_id = b.customer_id where a.order_status='COMPLETE' group by nombres order by cantidad_compras desc limit {top}"
        try:
            cursor.execute(sql)
            return cursor.fetchall()
        except Exception as e:
            return e
        finally:
            con.close