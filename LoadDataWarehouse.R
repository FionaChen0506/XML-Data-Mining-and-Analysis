# CS5200: Practicum 2
# Author: Shaoyujie(Fiona) Chen (chen.shaoy@northeastern.edu);
#         Yunke Li (li.yunke@northeastern.edu)
# Term: 2023 Summer Full


########## Load Required Libraries & clear environment ##########
# Packages loading
# Required Package names
packages <- c("RMySQL", "DBI", "RSQLite", "dplyr")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages], quiet = TRUE)
}

# Packages loading
suppressMessages(invisible(lapply(packages, library, character.only = TRUE)))

# Remove all objects to clear environment
rm(list = ls())



########## Connect to MySQL database ##########
mysqldb <-  dbConnect(RMySQL::MySQL(),
                      user = 'sql9638212',
                      password = 'bpz4ZNVWs9',
                      dbname = 'sql9638212',
                      host = 'sql9.freemysqlhosting.net',
                      port = 3306)


########## Connect to SQLite database ##########
fpath = "."
dbfile = "salesDB.sqlite"
dbcon <- dbConnect(RSQLite::SQLite(), file.path(fpath, dbfile))



########## Retrieve data from SQLite database and add Quarter infos #########
# Retrieve sales data from salestxn
sales_data <- dbGetQuery(dbcon, "
  SELECT txnID, prodID, custID, repID, date, amount,
         strftime('%m', date) AS Month,
         strftime('%Y', date) AS Year
  FROM salestxn
  ORDER BY txnID
")

# Create a Quarter column based on Month and Year
sales_data <- sales_data %>%
  mutate(Quarter = paste0("Q", ceiling(as.numeric(Month) / 3)))

# Change the data type of Month and Year to INT
sales_data$Month <- as.integer(sales_data$Month)
sales_data$Year <- as.integer(sales_data$Year)



########## A. Create and populate product_facts table in MySQL database ##########
# Retrieve relevant data
sales_amount_data <- sales_data %>%
  group_by(prodID, Year, Quarter, Month, custID) %>%
  summarize(totalSold = sum(amount))

products_data <- dbGetQuery(dbcon, "SELECT prodID, prod FROM products")
regions_data <- dbGetQuery(dbcon, "SELECT custID, country FROM customers")

# Left join sales_amount_data with products_data
product_facts_data <- sales_amount_data %>%
  left_join(products_data, by = "prodID")

# Left join the result with regions_data
product_facts_data <- product_facts_data %>%
  left_join(regions_data, by = "custID")

# Replace NA values with 0 in totalSold column
product_facts_data$totalSold[is.na(product_facts_data$totalSold)] <- 0


# Drop existing table
dbExecute(mysqldb, "DROP TABLE IF EXISTS product_facts")

# Create product_facts table in MySQL
query_create_product_facts <- "
    CREATE TABLE product_facts (
        prodID INT,
        prodName VARCHAR(255),
        year INT,
        quarter VARCHAR(30),
        month INT,
        region VARCHAR(255),
        totalSold INT
    );
"

dbExecute(mysqldb, query_create_product_facts)

# Populate product_facts table
for (i in 1:nrow(product_facts_data)) {
  query_insert_product_facts <- sprintf("
      INSERT INTO product_facts (prodID, prodName, year, quarter, month, region, totalSold)
      VALUES (%d, '%s', %d, '%s', %d, '%s', %d);
    ",
      product_facts_data$prodID[i],
      product_facts_data$prod[i],
      product_facts_data$Year[i],
      product_facts_data$Quarter[i],
      product_facts_data$Month[i],
      product_facts_data$country[i],
      product_facts_data$totalSold[i]
  )
  
  dbExecute(mysqldb, query_insert_product_facts)
}




########## B. Create and populate rep_facts table in MySQL database ##########
# Retrieve relevant data
sales_amount_data <- sales_data %>%
  group_by(repID, Year, Quarter, Month, prodID) %>%
  summarize(totalSold = sum(amount))
sales_amount_data$repID <- as.integer(sales_amount_data$repID)

products_data <- dbGetQuery(dbcon, "SELECT prodID, prod FROM products")
reps_data <- dbGetQuery(dbcon, "SELECT repID, firstName, lastName, territory FROM reps")

# Left join sales_amount_data with products_data
rep_facts_data <- sales_amount_data %>%
  left_join(products_data, by = "prodID")

# Left join the result with reps_data

rep_facts_data <- rep_facts_data %>%
  left_join(reps_data, by = "repID")

# Replace NA values with 0 in totalSold column
rep_facts_data$totalSold[is.na(rep_facts_data$totalSold)] <- 0

# Drop existing table
dbExecute(mysqldb, "DROP TABLE IF EXISTS rep_facts")

# Create rep_facts table in MySQL
query_create_rep_facts <- "
    CREATE TABLE rep_facts (
        repID INT,
        firstName VARCHAR(255),
        lastName VARCHAR(255),
        territory VARCHAR(255),
        year INT,
        quarter VARCHAR(30),
        month INT,
        prodName VARCHAR(255),
        totalSold INT
    );
"

dbExecute(mysqldb, query_create_rep_facts)

# Populate rep_facts table
for (i in 1:nrow(rep_facts_data)) {
  query_insert_rep_facts <- sprintf("
      INSERT INTO rep_facts (repID, firstName, lastName, territory, year, quarter, month, prodName, totalSold)
      VALUES (%d, '%s', '%s', '%s', %d, '%s', %d, '%s', %d);
    ",
      rep_facts_data$repID[i],
      rep_facts_data$firstName[i],
      rep_facts_data$lastName[i],
      rep_facts_data$territory[i],
      rep_facts_data$Year[i],
      rep_facts_data$Quarter[i],
      rep_facts_data$Month[i],
      rep_facts_data$prod[i],
      rep_facts_data$totalSold[i]
  )
  
  dbExecute(mysqldb, query_insert_rep_facts)
}



########### Testing ##########
# First 10 rows of the 2 fact tables
query_retrieve_product_facts <- "SELECT * FROM product_facts LIMIT 10"
retrieve_product_facts <- dbGetQuery(mysqldb, query_retrieve_product_facts)

query_retrieve_rep_facts <- "SELECT * FROM rep_facts LIMIT 10"
retrieve_rep_facts <- dbGetQuery(mysqldb, query_retrieve_rep_facts)



# Analytical queries
# 1. What is the total sold for each quarter of 2020 for all products?
query1 <- "
    SELECT quarter, SUM(totalSold) AS total_sold
    FROM product_facts
    WHERE year = 2020
    GROUP BY quarter;
"
result1 <- dbGetQuery(mysqldb, query1)



# 2. What is the total sold for each quarter of 2020 for 'Alaraphosol'?
query2 <- "
    SELECT quarter, SUM(totalSold) AS total_sold
    FROM product_facts
    WHERE year = 2020 AND prodName = 'Alaraphosol'
    GROUP BY quarter;
"
result2 <- dbGetQuery(mysqldb, query2)



# 3. Which product sold the best in 2020?
query3 <- "
    SELECT prodName, SUM(totalSold) AS total_sold
    FROM product_facts
    WHERE year = 2020
    GROUP BY prodName
    ORDER BY total_sold DESC
    LIMIT 1;
"
result3 <- dbGetQuery(mysqldb, query3)



# 4. How much did each sales rep sell in 2020?
query4 <- "
    SELECT repID, firstName, lastName, SUM(totalSold) AS total_sold
    FROM rep_facts
    WHERE year = 2020
    GROUP BY repID, firstName, lastName;
"
result4 <- dbGetQuery(mysqldb, query4)

# Print the result
print("First 10 row of product_facts: ")
print(retrieve_product_facts)
print("First 10 row of rep_facts: ")
print(retrieve_rep_facts)
print("Total sold for each quarter of 2020 for all products:")
print(result1)
print("Total sold for each quarter of 2020 for 'Alaraphosol':")
print(result2)
print("Product that sold the best in 2020:")
print(result3)
print("Total sales for each sales rep in 2020:")
print(result4)



########### Disconnect from database ##########
dbDisconnect(mysqldb)
dbDisconnect(dbcon)
