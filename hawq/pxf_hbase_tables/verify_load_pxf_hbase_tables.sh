#!/bin/bash
customers_dim_hbase=`psql -Atc "select count(*) from retail_demo.customers_dim_hbase;"`
categories_dim_hbase=`psql -Atc "select count(*) from retail_demo.categories_dim_hbase;"`    
customer_addresses_dim_hbase=`psql -Atc "select count(*) from retail_demo.customer_addresses_dim_hbase;"`       
date_dim_hbase=`psql -Atc "select count(*) from retail_demo.date_dim_hbase;"`           
email_addresses_dim_hbase=`psql -Atc "select count(*) from retail_demo.email_addresses_dim_hbase;"`
order_lineitems_hbase=`psql -Atc "select count(*) from retail_demo.order_lineitems_hbase;"`   
orders_hbase=`psql -Atc "select count(*) from retail_demo.orders_hbase;"`         
payment_methods_hbase=`psql -Atc "select count(*) from retail_demo.payment_methods_hbase;"`
products_dim_hbase=`psql -Atc "select count(*) from retail_demo.products_dim_hbase;"`

echo "							    "
echo "        Table Name            |   Count"
echo "------------------------------+------------------------"
echo " customers_dim_hbase          |   $customers_dim_hbase"
echo " categories_dim_hbase         |   $categories_dim_hbase"
echo " customer_addresses_dim_hbase |   $customer_addresses_dim_hbase"
echo " email_addresses_dim_hbase    |   $email_addresses_dim_hbase"
echo " order_lineitems_hbase        |   $order_lineitems_hbase"
echo " orders_hbase                 |   $orders_hbase"
echo " payment_methods_hbase        |   $payment_methods_hbase"
echo " products_dim_hbase           |   $products_dim_hbase"
echo "-----------------------------+------------------------"
