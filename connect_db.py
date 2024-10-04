import sqlite3
from pathlib import Path
from typing import Any
import re

class DatabaseConnect:
    """classe per la connessione al db
    """
    def __init__(self, db_path:Path):
        # Database setup
        self.db_path = db_path
        self.db_folder = ""
        self.db_name = ""
        self.connection = None
        self.cursor = None

        # Initialize the database
        self.init_database()

    def init_database(self):
        # Create necessary folder and database file if they don't exist
        self.db_folder = self.db_path.parent
        self.db_folder.mkdir(parents=True, exist_ok=True)

        if not self.db_path.exists():
            with open(self.db_path, "w"):
                pass

        # Connect to the database and create the table if it doesnt exist
        self.connector()

    def connector(self):
        # Establish a connection to the SQLite database
        self.connection = sqlite3.connect(self.db_path)
        self.cursor = self.connection.cursor()
        
    def common_update_execute(self, sql:str):
        # Execute common update SQL statements
        self.connector()
        try:
            self.cursor.execute(sql)
            # Commit changes to the database
            self.connection.commit()
        except Exception as e:
            # Rollback changes in case of an exception
            self.connection.rollback()
            return e
        finally:
            # Close the cursor and connection
            self.cursor.close()
            self.connection.close()

    def common_search_one_execute(self, sql:str):
        # Execute common search SQL statement for one result
        self.connector()
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            #column_names = [desc[0] for desc in self.cursor.description]
            return result#, column_names
        except Exception:
            return None
        finally:
            self.cursor.close()
            self.connection.close()

    def common_search_all_execute(self, sql:str):
        # Execute common search SQL statement for all result
        self.connector()
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            #column_names = [desc[0] for desc in self.cursor.description]
            return result#, column_names
        except Exception as e:
            print(e)
            return None
        finally:
            self.cursor.close()
            self.connection.close()

    def column_exists(self, db_name:str, table_name:str, column_name:str) -> bool:
        """
        Verifica se una colonna esiste nella tabella specificata.
        """
        db = db_name + '.' if db_name else db_name
        
        sql = f"""
        SELECT COUNT(*)
        FROM {db}pragma_table_info('{table_name}')
        WHERE name = '{column_name}';
        """
        
        column = self.common_search_one_execute(sql=sql)
        #print('column_exists', sql, column, '\n')
        return True if column[0] == 1 else False
    
    def tabel_exists(self, db_name:str, table_name:str) -> bool:
        """
        Verifica se una tabella esiste nel db specificato
        """
        db = db_name + '.' if db_name else db_name
                
        sql = f"SELECT name FROM {db}sqlite_master WHERE type='table' AND name='{table_name}'"
        
        result = self.common_search_one_execute(sql=sql)
        
        return True if result else False

    def get_table(self, db_name:str, table_name:str):
        if not self.tabel_exists(db_name, table_name):
            print(f"La tabella {table_name} non esiste.")
            return 
        
        # Execute common search SQL statement for all result
        db = db_name + '.' if db_name else db_name
        
        sql = f"SELECT * FROM {db}{table_name}"
        
        rows:list[tuple] = []
        #description:list[str] = []
        rows = self.common_search_all_execute(sql=sql)
        
        return rows
    
    def get_key_trasleted(self,  db_name:str, table_name:str, key_field:str, value_field:str, value:int):
        if not self.tabel_exists(db_name, table_name):
            print(f"La tabella {table_name} non esiste.")
            return 
        
        if not self.column_exists(db_name, table_name, key_field):
            print(f"La tabella {table_name} esiste, ma non esiste la colonna {key_field}.")
            return 
        
        if not self.column_exists(db_name, table_name, value_field):
            print(f"La tabella {table_name} esiste, ma non esiste la colonna {value_field}.")
            return 
        
        db = db_name + '.' if db_name else db_name
        
        sql = f"SELECT {value_field} FROM {db}{table_name} WHERE {key_field} = {value}" 
        result = self.common_search_one_execute(sql=sql)
                
        return result[0]

    def get_keys_trasleted(self, db_name:str, table_name:str, key_field:str, value_field:str, rows:list[tuple[int]]) -> list:
        results = []
        
        if not self.tabel_exists(db_name, table_name):
            print(f"La tabella {table_name} non esiste.")
            return results
        
        if not self.column_exists(db_name, table_name, key_field):
            print(f"La tabella {table_name} esiste, ma non esiste la colonna {key_field}.")
            return results
        
        if not self.column_exists(db_name, table_name, value_field):
            print(f"La tabella {table_name} esiste, ma non esiste la colonna {value_field}.")
            return results
        
        db = db_name + '.' if db_name else db_name
        for row in rows:
            sql = f"SELECT {value_field} FROM {db}{table_name} WHERE {key_field} = {row[0]}" 
            result = self.common_search_one_execute(sql=sql)
            results.append(result)
                
        return results
    
    def get_domain_data(self, db_name:str, table_name:str, key_field:str, value_field:str, column_to_check:str = 'VALIDA') -> list[tuple[int, str]]:
        """
        Estrae i domini dal database GPKG.
        """
        if not self.tabel_exists(db_name, table_name):
            print(f"La tabella {table_name} non esiste.")
            return 
        
        if not self.column_exists(db_name, table_name, key_field):
            print(f"La tabella {table_name} esiste, ma non esiste la colonna {key_field}.")
            return 
        
        if not self.column_exists(db_name, table_name, value_field):
            print(f"La tabella {table_name} esiste, ma non esiste la colonna {value_field}.")
            return 
        
        #db_name += '.' if db_name else ''
        # print(db_name)
        # Verifica se la colonna VALIDA esiste
        if self.column_exists(db_name, table_name, column_to_check):
            db = db_name + '.' if db_name else db_name
            sql = f'SELECT {key_field}, {value_field} FROM {db}{table_name} WHERE {column_to_check} = 1 ORDER BY {value_field}'
        else:
            db = db_name + '.' if db_name else db_name
            sql = f'SELECT {key_field}, {value_field} FROM {db}{table_name} ORDER BY {value_field}'
        #print(sql)
        
        rows:list[tuple[int, str]] = []
        rows = self.common_search_all_execute(sql=sql)

        return rows
  
    def get_columns_value(self,  db_name:str, table_name:str, key_field:str, fields:list[str], value:Any):
        if not self.tabel_exists(db_name, table_name):
            print(f"La tabella {table_name} non esiste.")
            return 
        
        if not self.column_exists(db_name, table_name, key_field):
            print(f"La tabella {table_name} esiste, ma non esiste la colonna {key_field}.")
            return 
        
        for field in fields:
            if not self.column_exists(db_name, table_name, field):
                print(f"La tabella {table_name} esiste, ma non esiste la colonna {field}.")
                return 
        
        db = db_name + '.' if db_name else db_name
        
        sql = f"SELECT " 
        sql += ', '.join(fields)
        sql += f" FROM {db}{table_name} WHERE {key_field} = '{value}'"
        result, columns_name = self.common_search_one_execute(sql=sql)
                
        return result, columns_name

    def get_layers_sdi(self, db_name:str) -> list[str]:
        """Restituisce una lista di nomi di layer a carico della SDI"""
        # Execute common search SQL statement for all result
        db = db_name + '.' if db_name else db_name
        
        sql = f"SELECT LAYER FROM {db}LAYER WHERE OWNER == 'SDI'"
        
        rows:list[tuple] = []
        rows = self.common_search_all_execute(sql=sql)
        
        if not rows:
            return None
        
        result: list[str] = [tupla[0] for tupla in rows]
        
        return result

    def get_layers_rg_cod(self, db_name:str) -> list[str]:
        """Restituisce una lista di nomi di layer per i quali calcolare l'rg_cod"""
        # Execute common search SQL statement for all result
        db = db_name + '.' if db_name else db_name
        
        sql = f"SELECT LAYER FROM {db}LAYER WHERE RG_COD == 1"
        
        rows:list[tuple] = []
        rows = self.common_search_all_execute(sql=sql)
        
        if not rows:
            return None
        
        result: list[str] = [tupla[0] for tupla in rows]
        
        return result

    def get_layer_prefix_suffix(self, db_name:str, layer_name:str) -> tuple[str, str]:
        """Restituisce il prefisso e il suffisso del layer o '', '' """
        # Execute common search SQL statement for all result
        db = db_name + '.' if db_name else db_name
        
        sql = f"SELECT PREFISSO, SUFFISSO FROM {db}LAYER WHERE LAYER == '{layer_name}'"
        
        result: tuple[str, str] = self.common_search_one_execute(sql=sql)
        
        if not result:
            return ('', '')
        
        prefisso, suffisso = result
        if not prefisso:
            prefisso = ''
        if not suffisso:
            suffisso = ''
        
        return prefisso, suffisso

    def get_layer_fields_type(self, db_name:str, layer_name:str) -> list[tuple[str, str]]:
        '''Restituisce una lista di tuple nome campo e tipo campo del layer'''
        # Execute common search SQL statement for all result
        db = db_name + '.' if db_name else db_name
        
        sql = f"SELECT FIELD, TYPE FROM {db}LAYER_FIELD_TYPE WHERE LAYER == '{layer_name}'"
        
        rows:list[tuple[str, str]] = []
        rows = self.common_search_all_execute(sql=sql)
        
        
        return rows
    
    def get_linked_layer(self, db_name:str, layer_name:str) -> list[tuple[str, str, str]]:
        '''Restituisce una lista di tuple del campo FK_FIELD collegato al campo LINKED_FIELD del layer LINKED_LAYER '''
        # Execute common search SQL statement for all result
        db = db_name + '.' if db_name else db_name
        
        sql = f"SELECT FK_FIELD, LINKED_LAYER, LINKED_FIELD FROM {db}LAYER_LINKED WHERE LAYER == '{layer_name}'"
        
        rows:list[tuple[str, str, str]] = []
        rows = self.common_search_all_execute(sql=sql)

        return rows
    
    def get_layers_with_attachments(self, db_name:str) -> list[str]:
        """Restituisce una lista di layer che hanno degli allegati"""
        # Execute common search SQL statement for all result
        db = db_name + '.' if db_name else db_name
        
        sql = f"select distinct LAYER from {db}LAYER_FIELD_TYPE where ATTACHED_DIR is not Null"
        
        
        rows: list[tuple[str]] = []
        rows = self.common_search_all_execute(sql=sql)
        
        if not rows:
            return None
        
        result: list[str] = [tupla[0] for tupla in rows]
        
        return result
    
    def get_layer_field_with_attach(self, db_name:str, layer_name:str) -> tuple[str, str, str]:
        """Restituisce una lista di tuple campo contenente il nome dell'allegato e cartella che lo dovrebbe contenere per il layer"""
        # Execute common search SQL statement for all result
        db = db_name + '.' if db_name else db_name
        
        sql = f"select FIELD, ATTACHED_DIR, ATTACHED_TYPE from {db}LAYER_FIELD_TYPE where LAYER == '{layer_name}' and ATTACHED_DIR is not Null"
        # print(sql)
        row:tuple[str, str, str] = ()
        row = self.common_search_one_execute(sql=sql)
        
        return row
    
    def get_layers_geometry_type(self, db_name:str, owner:str) -> list[str, str]:
        """Restituisce una lista di tuple nome del layer e tipo di geometria del layer"""
        # Execute common search SQL statement for all result
        db = db_name + '.' if db_name else db_name
        
        sql = f"select LAYER, GEOMETRY_TYPE from {db}LAYER where OWNER == '{owner}'"
        # print(sql)
        rows: list[tuple[str]] = []
        rows = self.common_search_all_execute(sql=sql)
        
        
        return rows
        
        

# db_obj = DatabaseConnect(Path("resources/db/db.gpkg"))

# layer_name = "SGA_INTERF_SEZ"
# db = ''
# sql = f"select FIELD, ATTACHED_DIR from {db}LAYER_FIELD_TYPE where LAYER == '{layer_name}' and ATTACHED_DIR is not Null"
# #rows = db_obj.common_search_all_execute(sql)
# rows = db_obj.get_layer_field_with_attach('', layer_name)
# print(rows)


    # def get_all_locations(self):
    #     # Retrieve distinct location form the table
    #     sql = f"SELECT DISTINCT location FROM {self.table_name};"
    #     result = self.common_search_all_execute(sql=sql)

    #     return result

    # def add_new_product(self, **kwargs):
    #     # Insert a new product into the table
    #     column_name = tuple(kwargs.keys())
    #     values = tuple(kwargs.values())

    #     sql = f"INSERT INTO {self.table_name} {column_name} VALUES {values};"
    #     result = self.common_update_execute(sql=sql)

    #     return result

    # def update_product(self, **kwargs):
    #     # Update product info in the table
    #     values = tuple(kwargs.values())

    #     sql = f""" UPDATE {self.table_name} SET 
    #                 cost={kwargs["cost"]}, 
    #                 price={kwargs["price"]}, 
    #                 location={kwargs["location"]}, 
    #                 reorder_level={kwargs["reorder_level"]}, 
    #                 stock={kwargs["stock"]}
    #                WHERE product_name='{values[0]}';
    #         """

    #     result = self.common_update_execute(sql=sql)
    #     return result

    # def get_data(self, search_flag="", product_name=""):
    #     """
    #         search_flag: ALL, IN_STOCK, RE_ORDER, NO_STOCK
    #     """
    #     if search_flag == "ALL":
    #         sql = f"SELECT product_name, cost, price, stock, location, reorder_level FROM {self.table_name}"
    #     elif search_flag == "IN_STOCK":
    #         sql = (f"SELECT product_name, cost, price, stock, location, reorder_level FROM {self.table_name} "
    #                f"WHERE stock>0")
    #     elif search_flag == "NO_STOCK":
    #         sql = (f"SELECT product_name, cost, price, stock, location, reorder_level FROM {self.table_name} "
    #                f"WHERE stock<=0")
    #     elif search_flag == "RE_ORDER":
    #         sql = (f"SELECT product_name, cost, price, stock, location, reorder_level FROM {self.table_name} "
    #                f"WHERE stock<=reorder_level")
    #     else:
    #         sql = (f"SELECT product_name, cost, price, stock, location, reorder_level FROM {self.table_name} "
    #                f"WHERE product_name='{product_name}'")

    #     # Execute the SQL statement
    #     result = self.common_search_all_execute(sql=sql)
    #     return result

    # def delete_product(self, product_name):
    #     # Delete a product from the table
    #     sql = f"DELETE from {self.table_name} WHERE product_name='{product_name}'"
    #     result = self.common_update_execute(sql=sql)
    #     return result

    # def get_product_names(self, product_name):
    #     # Retrieve product names based on a partial or complete name
    #     sql = f"SELECT product_name FROM {self.table_name} WHERE product_name LIKE '%{product_name}%'"
    #     search_result = self.common_search_all_execute(sql=sql)
    #     return search_result

    # def get_single_product_info(self, product_name):
    #     # Retrieve information about a single product
    #     sql = f"SELECT stock, reorder_level, location FROM {self.table_name} WHERE product_name='{product_name}'"
    #     search_result, _ = self.common_search_one_execute(sql=sql)
    #     return search_result

    # def get_current_stock(self):
    #     # Retrieve the sum of all stocks in the table
    #     sql = f"SELECT sum(stock) FROM {self.table_name}"
    #     search_result, _ = self.common_search_one_execute(sql=sql)
    #     return search_result

    # def get_stock_value(self):
    #     # Retrieve the sum of the value of all stocks in the table
    #     sql = f"SELECT sum(price*stock) FROM {self.table_name}"
    #     search_result, _ = self.common_search_one_execute(sql=sql)
    #     return search_result

    # def get_stock_cost(self):
    #     # Retrieve the sum of the cost of all stocks in the table
    #     sql = f"SELECT sum(cost*stock) FROM {self.table_name}"
    #     search_result, _ = self.common_search_one_execute(sql=sql)
    #     return search_result

    # def get_reorder_product(self):
    #     # Retrieve the count of products that need to be reordered
    #     sql = f"SELECT count(*) FROM {self.table_name} WHERE reorder_level>=stock "
    #     search_result, _ = self.common_search_one_execute(sql=sql)
    #     return search_result

    # def get_no_stock_product(self):
    #     # Retrieve the count of products with no stock
    #     sql = f"SELECT count(*) FROM {self.table_name} WHERE stock<=0 "
    #     search_result, _ = self.common_search_one_execute(sql=sql)
    #     return search_result

    # def update_stock(self, **kwargs):
    #     # Update the stock of a product
    #     sql = f""" UPDATE {self.table_name} SET 
    #                 stock={kwargs["stock"]}
    #                WHERE product_name='{kwargs["product_name"]}';
    #             """

    #     # Execute the SQL statement
    #     result = self.common_update_execute(sql)
    #     return result




# keys_values = {'a': 0, 'b':1, 'c':2}
# columns_values_list = []
# for k, v in keys_values.items():
#     columns_values_list.append(f"'{k}' = {v}")
# where_clause = " AND ".join(columns_values_list)

# sql = f"""
# SELECT *
# FROM SGA_SEZIONE_SCAVO_PARAM
# WHERE {where_clause}
# """

# print(sql)
