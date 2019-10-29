# Author: José LISE
# August 2019
# Data Wrangling with SQL mini project 
#
# Load packages.
import numpy as np
import pandas as pd
import revoscalepy as revoscale #used as DB Connection framework, in MSSQL Python env
from timeit import default_timer as timer # Used to measure the CPU Time of the Division process

def DoInMemoryDivision():
    # Connection string to connect to local DB
    conn_str = 'Driver=SQL Server;Server=DESKTOP-M98I69D;Database=S19SQLPlayground_Seb;Trusted_Connection=True;'
    # Queries to retrieve the Customers, Products, and purchases data respectively
    query_customers = '''SELECT * FROM dbo.Customer'''
    query_products = '''SELECT * FROM dbo.Product'''
    query_purchases = '''SELECT * FROM dbo.Purchase'''

    # Define the columns we wish to import for Customers, Products, and purchases data respectively
    col_cust_info = {
        "Customer": {"type": "integer"},
        "FirstName": {"type": "str"},
        "LastName": {"type": "str"}
    }
    col_prod_info = {
        "Product": {"type": "integer"},
        "Description": {"type": "str"},
        "UnitPrice": {"type": "integer"}
    }
    col_pur_info = {
        "Customer": {"type": "integer"},
        "Product": {"type": "integer"},
        "Quantity": {"type": "integer"}
    }

    # Start Timer to measure execution time
    start = timer()

    data_source_cust = revoscale.RxSqlServerData(sql_query=query_customers, column_Info=col_cust_info,
                                              connection_string=conn_str)
    data_source_prod = revoscale.RxSqlServerData(sql_query=query_products, column_Info=col_prod_info,
                                              connection_string=conn_str)
    data_source_pur = revoscale.RxSqlServerData(sql_query=query_purchases, column_Info=col_pur_info,
                                              connection_string=conn_str)
    # retrieve the data sources 
    revoscale.RxInSqlServer(connection_string=conn_str, num_tasks=3, auto_cleanup=False)
    # import data sources and convert to pandas dataframe.
    customer_data = pd.DataFrame(revoscale.rx_import(data_source_cust))
    product_data = pd.DataFrame(revoscale.rx_import(data_source_prod))
    purchase_data = pd.DataFrame(revoscale.rx_import(data_source_pur))

    # Create a DataFrame to store customer purchase count
    cust_dict = {'CustId':[], 'PurchaseCount':[] }
    cust_Purc_Count = pd.DataFrame(cust_dict)

    # Nb of products
    nb_products = len(product_data.index)
    # Division process 
    # Iterate over all customers by df rows
    for cindex, crow in customer_data.iterrows():
        # Iterate over all products by df rows
        for pindex,prow in product_data.iterrows():
            # Iterate over all purchases by df rows
            for hindex,hrow in purchase_data.iterrows():
                # Check whether the currentCustomer has bought the current product
                # print (crow['CustomerId'], hrow['CustomerId'], prow['ProductId'] )
                if (crow['CustomerId'] == hrow['CustomerId']) and (prow['ProductId'] == hrow['ProductId']):
                    # Either add a new DataRow in custPurchasesCount
                    # if no existing for the current customer or update the count
                    custExistence = 0
                    for index,cust_count in cust_Purc_Count.iterrows(): 
                        if cust_count['CustId'] == crow['CustomerId']:
                            custExistence +=1

                    if (custExistence == 0):       
                        # First product for this customer
                        td = {'CustId':crow['CustomerId'], 'PurchaseCount':1}
                        # Make a dataframe to be able to append to cust_Purc_Count
                        tdf = pd.DataFrame(td, index=[0])
                        # Append to cust_Purc_Count
                        # We must use the syntax below because for df append doesn't occur in place but in the object returned by the method
                        cust_Purc_Count = cust_Purc_Count.append(tdf)
                    else:
                        # The product is already referenced cust_Purc_Count => Just increase the PurchaseCount  
                        cust_count['PurchaseCount'] += 1


    for dindex, drow in cust_Purc_Count.iterrows():
        # Check the customers that bought all the products
        if drow['PurchaseCount']  == nb_products:
            print ('\nDivision CustomerId: '+ str(drow['CustId']))


    end = timer()
    print( "The In Memory Division in Python took (S) : ", end - start)

DoInMemoryDivision()

