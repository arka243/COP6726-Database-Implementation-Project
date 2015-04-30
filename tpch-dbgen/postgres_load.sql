COPY region FROM '/tmp/region.tbl' WITH DELIMITER '|';
COPY nation FROM '/tmp/nation.tbl' WITH DELIMITER '|';
COPY customer FROM '/tmp/customer.tbl' WITH DELIMITER '|';
COPY supplier FROM '/tmp/supplier.tbl' WITH DELIMITER '|';
COPY part FROM '/tmp/part.tbl' WITH DELIMITER '|';
COPY partsupp FROM '/tmp/partsupp.tbl' WITH DELIMITER '|';
COPY "orders" FROM '/tmp/orders.tbl' WITH DELIMITER '|';
COPY lineitem FROM '/tmp/lineitem.tbl' WITH DELIMITER '|';

ANALYSE region;
ANALYSE nation;
ANALYSE customer;
ANALYSE supplier;
ANALYSE part;
ANALYSE partsupp;
ANALYSE orders;
ANALYSE lineitem;




