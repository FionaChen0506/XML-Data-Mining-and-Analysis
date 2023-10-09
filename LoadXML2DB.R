# CS5200: Practicum 2
# Author: Shaoyujie(Fiona) Chen (chen.shaoy@northeastern.edu);
#         Yunke Li (li.yunke@northeastern.edu)
# Term: 2023 Summer Full

# Load the packages
library(XML)
library(knitr)
library(RSQLite)


# define the file path and database 
fpath = "."
dbfile = "salesDB.sqlite"

# use file.path() to concatenate the directory path and file name.
dbcon <- dbConnect(RSQLite::SQLite(), file.path(fpath, dbfile))

## Question 1 & 2:
### Drop existing tables
rs <- dbExecute(dbcon, "DROP TABLE IF EXISTS salestxn")
rs <- dbExecute(dbcon, "DROP TABLE IF EXISTS products")
rs <- dbExecute(dbcon, "DROP TABLE IF EXISTS reps")
rs <- dbExecute(dbcon, "DROP TABLE IF EXISTS customers")

###  Create required tables.
create_products_table <- "CREATE TABLE products (
  prodID INTEGER NOT NULL PRIMARY KEY,
  prod VARCHAR(255) NOT NULL UNIQUE
)"
rs <- dbExecute(dbcon, create_products_table)

create_reps_table <- "CREATE TABLE reps (
    repID INTEGER NOT NULL PRIMARY KEY,
    firstName VARCHAR(255) NOT NULL,
    lastName VARCHAR(255) NOT NULL,
    territory VARCHAR(255) NOT NULL
)"
rs <- dbExecute(dbcon, create_reps_table)

create_customers_table <- "CREATE TABLE customers (
    custID INTEGER NOT NULL PRIMARY KEY,
    cust VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(255)NOT NULL
)"
rs <- dbExecute(dbcon, create_customers_table)

create_salestxn_table <- "CREATE TABLE salestxn (
    txnID INT NOT NULL PRIMARY KEY, 
    prodID INT NOT NULL,
    custID INT NOT NULL,
    repID VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    qty INT NOT NULL,
    amount INT NOT NULL,
    FOREIGN KEY (prodID) REFERENCES products(prodID),
    FOREIGN KEY (custID) REFERENCES customers(custID),
    FOREIGN KEY (repID) REFERENCES reps(repID)
)"
rs <- dbExecute(dbcon, create_salestxn_table)

## Question 3 & Question 4:
### Load XML files


# Define the path and name of xml files
xml_path <- "txn-xml"
reps_file <- "pharmaReps.xml"

# Reading the XML files and parse into DOM
reps_doc <- xmlParse(file.path(xml_path, reps_file))

# Get the root nodes of the DOM trees
reps_root <- xmlRoot(reps_doc)

# Get number of <rep> nodes
n <- xmlSize(reps_root)

### Import the data into reps table.
# Create vectors to store attribute values
repIDs <- character(n)
firstNames <- character(n)
lastNames <- character(n)
territories <- character(n)

# Extract attributes and fill vectors
for (i in 1:n) {
  rep_node <- xmlChildren(reps_root)[[i]]
  repIDs[i] <- sub("^r", "", xmlGetAttr(rep_node, "rID"))  # Remove "r" prefix
  firstNames[i] <- xmlValue(rep_node[["firstName"]])
  lastNames[i] <- xmlValue(rep_node[["lastName"]])
  territories[i] <- xmlValue(rep_node[["territory"]])
}

# Create reps data frame
reps_data <- data.frame(
  repID = repIDs,
  firstName = firstNames,
  lastName = lastNames,
  territory = territories,
  stringsAsFactors = FALSE
)


# Write data to the reps table in the database
dbWriteTable(dbcon, "reps", reps_data, row.names = FALSE, append = TRUE)

# Define a list of salestxn XML files
salestxn_files <- list.files(xml_path, pattern = "pharmaSalesTxn.*\\.xml", full.names = TRUE)
# Loop through each salestxn file and import data
for (salestxn_file in salestxn_files) {
  #cat("Processing file:", salestxn_file, "\n") 
  # Reading the XML file and parse into DOM
  salestxn_doc <- xmlParse(salestxn_file)
  
  # Get the root node of the DOM tree
  salestxn_root <- xmlRoot(salestxn_doc)
  
  ### Import the data into products table.
  
  # Get the last used prodID from the products table
  last_prodID  <- dbGetQuery(dbcon, "SELECT MAX(prodID) FROM products")
  if (is.na(last_prodID)) {
    last_prodID <- 0
  } else {
    last_prodID <- as.integer(last_prodID)
  }
  
  # Extract unique products and customers data from the sales transaction XML
  unique_products <- unique(xpathSApply(salestxn_root, "//prod", xmlValue))
  
  # Check for existing products in the table and remove them from the data frame
  existing_products <- dbGetQuery(dbcon, "SELECT prod FROM products")
  new_products <- unique_products[!unique_products %in% existing_products$prod]
  
  # Create products data frame
  products_data <- data.frame(
    prodID = seq(last_prodID + 1, length.out = length(new_products)),
    prod = new_products,
    stringsAsFactors = FALSE
  )
  
  # Write data to the products table in the database
  dbWriteTable(dbcon, "products", products_data, row.names = FALSE, append = TRUE)
  
  
  ### Import the data into customers table.
  # Get the last used custID from the customers table
  last_custID <- dbGetQuery(dbcon, "SELECT MAX(custID) FROM customers")
  if (is.na(last_custID)) {
    last_custID <- 0
  } else {
    last_custID <- as.integer(last_custID)
  }
  
  # Extract unique customers data from the sales transaction XML
  unique_customers <- unique(xpathSApply(salestxn_root, "//cust", xmlValue))
  
  # Check for existing customers in the table and remove them from the data frame
  existing_customers <- dbGetQuery(dbcon, "SELECT cust FROM customers")
  new_customers <- unique_customers[!unique_customers %in% existing_customers$cust]
  
  # Create customers data frame
  customers_data <- data.frame(
    custID = seq(last_custID + 1, length.out = length(new_customers)),
    cust = new_customers,
    country = character(length(new_customers)),
    stringsAsFactors = FALSE
  )
  
  # Fill the 'country' column in the customers data frame based on cust names
  for (i in seq_len(nrow(customers_data))) {
    cust_name <- customers_data$cust[i]
    suppressWarnings({
      customers_data$country[i] <- xpathSApply(salestxn_root, paste0("//cust[text()='", cust_name, "']/following-sibling::country[1]"), xmlValue)
    })  
  }
  
  # Write data to the customers table in the database
  dbWriteTable(dbcon, "customers", customers_data, row.names = FALSE, append= TRUE)
  
  
  ### Import the data into salestxn table.
  # Get number of <salestxn> nodes
  n_salestxn <- xmlSize(salestxn_root)
  
  # Get the last used txnID from the salestxn table
  last_txnID <- dbGetQuery(dbcon, "SELECT MAX(txnID) FROM salestxn")
  if (is.na(last_txnID)) {
    last_txnID <- 0
  } else {
    last_txnID <- as.integer(last_txnID)
  }
  
  # Create vectors to store attribute values
  prodNames <- character(n_salestxn)
  custNames <- character(n_salestxn)
  dates <- character(n_salestxn)
  qtys <- integer(n_salestxn)
  amounts <- integer(n_salestxn)
  repIDs <- character(n_salestxn)
  
  # Extract attributes and fill vectors
  for (i in 1:n_salestxn) {
    txn_node <- xmlChildren(salestxn_root)[[i]]
    prodNames[i] <- xmlValue(txn_node[["prod"]])
    custNames[i] <- xmlValue(txn_node[["cust"]])
    dates[i] <- xmlValue(txn_node[["date"]])
    qtys[i] <- as.integer(xmlValue(txn_node[["qty"]]))
    amounts[i] <- as.integer(xmlValue(txn_node[["amount"]]))
    repIDs[i] <- xmlValue(txn_node[["repID"]])
  }
  
  # Create salestxn data frame with initially empty columns for prodID and custID
  salestxn_data <- data.frame(
    txnID = seq(last_txnID + 1, length.out = n_salestxn),
    prodID = integer(n_salestxn),
    custID = integer(n_salestxn),
    repID = repIDs,
    date = dates,
    qty = qtys,
    amount = amounts,
    cust = custNames,
    prod = prodNames,
    stringsAsFactors = FALSE
  )
  
  # Convert the dates to proper date format (assuming the format is "mm/dd/yyyy")
  salestxn_data$date <- as.character(as.Date(salestxn_data$date, format = "%m/%d/%Y"))
  
  # Function to get prodID from products table based on prod name
  getProductID <- function(prod_name) {
    query <- paste0("SELECT prodID FROM products WHERE prod='", prod_name, "'")
    result <- dbGetQuery(dbcon, query)
    return(result$prodID)
  }
  
  
  # Function to get custID from customers table based on cust name
  getCustomerID <- function(cust_name) {
    query <- paste0("SELECT custID FROM customers WHERE cust='", cust_name, "'")
    result <- dbGetQuery(dbcon, query)
    return(result$custID)
  }
  
  # Fill in the prodID and custID columns based on the information from the XML
  for (i in seq_len(nrow(salestxn_data))) {
    prod_name <- salestxn_data$prod[i]
    cust_name <- salestxn_data$cust[i]
    salestxn_data$prodID[i] <- getProductID(prod_name)
    salestxn_data$custID[i] <- getCustomerID(cust_name)
  }
  
  # Remove the 'prod' and 'cust' columns from salestxn_data
  salestxn_data <- subset(salestxn_data, select = -c(prod, cust))
  
  # Write data to the salestxn table in the database
  dbWriteTable(dbcon, "salestxn", salestxn_data, row.names = FALSE, append = TRUE)
  
  cat("Finished processing:", salestxn_file, "\n") 
  
}

# To test the result of table population.
# reps_query <- paste0("SELECT * FROM reps")
# reps_result <- dbGetQuery(mydb, reps_query)
# print(reps_result)


dbDisconnect(dbcon)