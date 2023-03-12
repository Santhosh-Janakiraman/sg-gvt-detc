
DROP TABLE  IF EXISTS tTransactionDetails;
CREATE TABLE IF NOT EXISTS tTransactionDetails  
(
	 TrnId int
	,MembershipId varchar(20)
	,ItemCode int
	,TrnDate timestamp
	,StatusCode varchar(20)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tTransactionDetails 
ADD CONSTRAINT  pk_tTransactionDetails_TrnId PRIMARY KEY(TrnId);

ALTER TABLE tTransactionDetails 
ADD CONSTRAINT  pk_tTransactionDetails_ItemCode FOREIGN KEY(ItemCode) REFERENCES tItemDetails(ItemCode)
--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tItemDetails;
CREATE TABLE IF NOT EXISTS tItemDetails
(
	 ItemCode int
	,Manufacturer_Code int
	,Cost decimal(26,6)
	,Weight decimal(26,6)
	,WeightScaleCode varchar(20)
	,ValidFromDate timestamp
	,ValidToDate timestamp
	,IsActive varchar(1)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tItemDetails 
ADD CONSTRAINT  pk_tItemDetails_ItemCode PRIMARY KEY(ItemCode);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tPaymentDetails;
CREATE TABLE tPaymentDetails
(
	 PaymentId int
	,PaymentType varchar(20)
	,PaymentStatusCode varchar(20)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tPaymentDetails 
ADD CONSTRAINT  pk_tPaymentDetails_PaymentId PRIMARY KEY(PaymentId);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tMasterManufacturerDetails;
CREATE TABLE IF NOT EXISTS tMasterManufacturerDetails
(
	 ManufacturerId int
	,Name varchar(100)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tMasterManufacturerDetails 
ADD CONSTRAINT  pk_tMasterManufacturerDetails_ManufacturerId PRIMARY KEY(ManufacturerId);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tMasterContactDetails;
CREATE TABLE IF NOT EXISTS tMasterContactDetails
(
	 ContactId int
	,ContactPersonId int
	,ContactTypeCode varchar(20)
	,IsPrimary varchar(1)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tMasterContactDetails 
ADD CONSTRAINT  pk_tMasterContactDetails_ContactId PRIMARY KEY(ContactId);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tMasterUserDetails;
CREATE TABLE IF NOT EXISTS tMasterUserDetails
(
	 UserId int
	,UserType varchar(20)
	,MembershipId varchar(20)
	,FirstName varchar(100)
	,LastName varchar(100)
	,EmailAddress varchar(100)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tMasterUserDetails 
ADD CONSTRAINT  pk_tMasterUserDetails_UserId PRIMARY KEY(UserId);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tMasterAddressDetails;
CREATE TABLE IF NOT EXISTS tMasterAddressDetails
(
	 AddressId int
	,AddressLine_1 varchar(100)
	,AddressLine_2 varchar(100)
	,PostalCode varchar(20)
	,AddressTypeCode varchar(20)
	,IsPrimary varchar(1)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tMasterAddressDetails 
ADD CONSTRAINT  pk_tMasterAddressDetails_AddressId PRIMARY KEY(AddressId);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tRefContactType;
CREATE TABLE IF NOT EXISTS tRefContactType
(
	 ContactId int
	,ContactTypeCode varchar(20)
	,ContactTypeDesc varchar(100)
	,IsActive varchar(1)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tRefContactType 
ADD CONSTRAINT  pk_tRefContactType_ContactId PRIMARY KEY(ContactId);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tRefWeightScale;
CREATE TABLE IF NOT EXISTS tRefWeightScale
(
	 ScaleCode varchar(20)
	,ScaleDesc varchar(100)
	,IsActive varchar(1)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tRefWeightScale 
ADD CONSTRAINT  pk_tRefWeightScale_ScaleCode PRIMARY KEY(ScaleCode);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tRefAddressType;
CREATE TABLE IF NOT EXISTS tRefAddressType
(
	 AddressTypeCode varchar(20)
	,AddressType varchar(100)
	,IsActive varchar(1)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tRefAddressType 
ADD CONSTRAINT  pk_tRefAddressType_AddressTypeCode PRIMARY KEY(AddressTypeCode);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tRefTranscationStatus;
CREATE TABLE IF NOT EXISTS tRefTranscationStatus
(
	 StatusCode varchar(20)
	,StatusDesc varchar(100)
	,IsActive varchar(1)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tRefTranscationStatus 
ADD CONSTRAINT  pk_tRefTranscationStatus_StatusCode PRIMARY KEY(StatusCode);

--------------------------------------------------------------------------------

DROP TABLE  IF EXISTS tRefPaymentStatus;
CREATE TABLE IF NOT EXISTS tRefPaymentStatus
(
	 StatusCode varchar(20)
	,StatusDesc varchar(100)
	,CreatedBy varchar(20)
	,CreatedDate timestamp
	,UpdatedBy varchar(20)
	,UpdatedDate timestamp
);

ALTER TABLE tRefPaymentStatus 
ADD CONSTRAINT  pk_tRefPaymentStatus_StatusCode PRIMARY KEY(StatusCode);




ALTER TABLE tTransactionDetails 
ADD CONSTRAINT  pk_tTransactionDetails_ItemCode FOREIGN KEY(ItemCode) REFERENCES tItemDetails(ItemCode)

ALTER TABLE tTransactionDetails 
ADD CONSTRAINT  pk_tTransactionDetails_MembershipId FOREIGN KEY(MembershipId) REFERENCES tMasterUserDetails(MembershipId)

ALTER TABLE tTransactionDetails 
ADD CONSTRAINT  pk_tTransactionDetails_StatusCode FOREIGN KEY(StatusCode) REFERENCES tRefTranscationStatus(StatusCode)

ALTER TABLE tTransactionDetails 
ADD CONSTRAINT  pk_tTransactionDetails_CreatedBy FOREIGN KEY(CreatedBy) REFERENCES tMasterUserDetails(UserId)

ALTER TABLE tTransactionDetails 
ADD CONSTRAINT  pk_tTransactionDetails_UpdatedBy FOREIGN KEY(UpdatedBy) REFERENCES tMasterUserDetails(UserId)

---

ALTER TABLE tItemDetails 
ADD CONSTRAINT  pk_tItemDetails_ManufacturerCode FOREIGN KEY(ManufacturerCode) REFERENCES tItemDetails(ItemCode)

