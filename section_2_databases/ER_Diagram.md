
# ER Diagram

## Tables 

1. Transaction tables
   - Transaction Details [tTransactionDetails]
     - This is the primary transaction table to capture the transaction on the ecommerce system 
   - Payment Details [tPaymentDetails]
     - Captures the payment information per tranaction and can enteries per transaction handle mutiple payment attempts.
     - The file status based onthe workfolw will be synced with transaction table
     - Payment Status value references the list in [tRefPaymentStatus] table
2. Master Tables
   1. Item Details [tMasterItemDetails]
      - Use to maintian the list of items available and their cost
      - The value of this table will be timelined between the ValidFromDate and ValidToDate column
      - IsActive table is managed to de-activate the product upon expiry, whcih should be handled by an trigger or any common function module. 
      - ManufacturerId value reference thes list in [tMasterManufacturerDetails] table
   2. Manufacturer Details [tMasterManufacturerDetails]
      - Used  to capture all the Manufacturer for the Items availble in eCommerece
   3. User Details [tMasterUserDetails]
       - Used to maintain all types of users. Assumption on user types are 
         - End Customer [has valid membership id]
         - Internal Users or employees [should be defaulted to user id]
   4. Address Details [tMasterAddressDetails]
      - User to maintain the master lisf of addresses for manufacturer and the users 
      - A Manufacturer or an user can have multiple addressess 
   5. Contact Details [tMasterContactDetails]
      - User to maintain the master lisf of contacts for manufacturer and the users 
      - A Manufacturer or an user can have multiple contact details 
3. Reference Tables
   1. Contact Type [tRefContactType]
      - Type of contact such as handphone, landline, fax , office addresses by branch, etc
   2. Address Type [tRefAddressType]
      - Types of address such as home address, office address, differnt business unit addresses, etc 
   3. Weight Scale [tRefWeightScale]
      - Used to tract differnt types of weight scales per item. 
      - Can be used for conversions if needed
   4. Transction Status [tRefTransctionStatus]
      - Refernce for various tranaction statuses such as 
        - failed
        - successful
        - on hold
        - expired 
        - aborted
        - cancelled etc.
   5. Payment Status [tRefPaymentStatus]
      - Referene for various payment status such as
        - failed
        - successful
        - cancelled
        - in process 

## Docker Files 

1. Docker file included in the reppo
   - Path: section_2_databases/docker-compose.yaml
   - Execution steps:
     - ensure your docker demon is runnint and execute  below comamnd in bash/zsh. platform is necessary while running in arm/m1 mac   
       - docker-compose up --platform=linux/amd64 

## Scripts:

1. Scripts to create the tables in postgres db are included in repo
   - Path: section_2_databases/docker_postgres_init.sql

## Refernce ER Diagram

<img src="ER-Diagram.png" width=800 /> ![](ER-Diagram.png) 

