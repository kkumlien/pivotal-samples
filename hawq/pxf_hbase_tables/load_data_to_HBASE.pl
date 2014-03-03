#!/usr/bin/env perl

# Takes ~ 10 minutes to run
#
# You can clean up these tables using the hbase shell:
#  hbase(main):002:0> disable_all '^.+$'
#  hbase(main):003:0> drop_all '^.+$'

use strict;

#
# To get past the issue:
#
#   java.lang.NoClassDefFoundError: com/google/common/base/Function,
#
# I had to add one JAR to HADOOP_CLASSPATH in hadoop-env.sh:
#   $HBASE_ROOT/lib/guava-r06.jar (Value of `LIB_JARS', below)
#

my $HBASE_ROOT = "/usr/lib/gphd/hbase";
my $HBASE_JAR = "$HBASE_ROOT/hbase.jar";
my $LIB_JARS = "$HBASE_ROOT/lib/guava-11.0.2.jar";
my $DATA_DIR = "/retail_demo";

my %table_to_col = (
  # ROWKEY: category_id
  categories_dim => [qw(
    category_name
  )],
  # ROWKEY: customer_id
  customers_dim => [qw(
    first_name
    last_name
    gender
  )],
  # ROWKEY:
  order_lineitems => [qw(
    order_id
    order_item_id
    product_id
    product_name
    customer_id
    store_id
    item_shipment_status_code
    order_datetime
    ship_datetime
    item_return_datetime
    item_refund_datetime
    product_category_id
    product_category_name
    payment_method_code
    tax_amount
    item_quantity
    item_price
    discount_amount
    coupon_code
    coupon_amount
    ship_address_line1
    ship_address_line2
    ship_address_line3
    ship_address_city
    ship_address_state
    ship_address_postal_code
    ship_address_country
    ship_phone_number
    ship_customer_name
    ship_customer_email_address
    ordering_session_id
    website_url
  )],
  # ROWKEY: order_id
  orders => [qw(
    customer_id
    store_id
    order_datetime
    ship_completion_datetime
    return_datetime
    refund_datetime
    payment_method_code
    total_tax_amount
    total_paid_amount
    total_item_quantity
    total_discount_amount
    coupon_code
    coupon_amount
    order_canceled_flag
    has_returned_items_flag
    has_returned_items_flag
    has_returned_items_flag
    fraud_code
    fraud_resolution_code
    billing_address_line1
    billing_address_line2
    billing_address_city
    billing_address_state
    billing_address_postal_code
    billing_address_country
    billing_phone_number
    customer_name
    customer_email_address
    ordering_session_id
    website_url
  )],
  # ROWKEY: customer_address_id
  # Import took 6:54
  customer_addresses_dim => [qw(
    customer_id
    valid_from_timestamp
    valid_to_timestamp
    house_number
    street_name
    appt_suite_no
    city
    state_code
    zip_code
    zip_plus_four
    country
    phone_number
  )],
  # ROWKEY:
  date_dim => [qw(
    calendar_day
    reporting_year
    reporting_quarter
    reporting_month
    reporting_week
    reporting_dow
  )],
  # ROWKEY: customer_id
  email_addresses_dim => [qw(
    email_address
  )],
  # ROWKEY: payment_method_id
  payment_methods => [qw(
    payment_method_code
  )],
  # ROWKEY: product_id
  products_dim => [qw(
    category_id
    price
    product_name
  )],
);

foreach my $table (keys %table_to_col)
{
  create_hbase_table($table);
  import_to_hbase($table);
}

sub create_hbase_table {
  my $t_name = shift;
  print "Creating HBase table '$t_name' ...\n";
  open HBASE, "| hbase shell" or die $!;
  print HBASE "create '$t_name', 'cf1'\n";
  close HBASE;
}

sub import_to_hbase {
  my $t_name = shift;
  print "Running the 'importtsv' map/reduce job to load '$t_name' into HBase ...\n";
  my $cols = join(",", map { "cf1:$_" } @{$table_to_col{$t_name}});
  my $cmd = "hadoop jar $HBASE_JAR importtsv -libjars $LIB_JARS";
  $cmd .= " -Dimporttsv.columns=HBASE_ROW_KEY,$cols";
  $cmd .= " $t_name $DATA_DIR/$t_name";
  print "\n-------\n";
  print $cmd;
  print "\n";
  system $cmd;
}
