#You will need to do some work on the products table before casting the data types correctly.
#The product_price column has a £ character which you need to remove using SQL.
#The team that handles the deliveries would like a new human-readable column added for the weight so they can quickly make decisions on delivery weights.
#Add a new column weight_class which will contain human-readable values based on the weight range of the product.

#+--------------------------+-------------------+
#| weight_class VARCHAR(?)  | weight range(kg)  |
#+--------------------------+-------------------+
#| Light                    | < 2               |
#| Mid_Sized                | >= 2 - < 40       |
#| Heavy                    | >= 40 - < 140     |
#| Truck_Required           | => 140            |
#+----------------------------+-----------------+
