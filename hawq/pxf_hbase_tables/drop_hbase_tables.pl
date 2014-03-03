#!/usr/bin/env perl

my %tables = (
 "categories_dim",        
 "customers_dim",       
 "order_lineitems",
 "orders",
 "customer_addresses_dim",
 "date_dim",
 "email_addresses_dim",
 "payment_methods",
 "products_dim"
 );


foreach my $table (%tables){
  print "Dropping HBase table '$table' ...\n";
  open HBASE, "| hbase shell" or die $!;
  print HBASE "disable '$table'\n";
  print HBASE "drop '$table'\n";
  close HBASE;
}
