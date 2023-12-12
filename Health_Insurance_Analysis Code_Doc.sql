-- Find the version of Hadoop
hdfs version

-- Find the CPU speed
lscpu

-- Find the number of CPU cores
nproc

-- Find the number of nodes listed (3), only shows working nodes, not showing master nodes
yarn node -list -all

-- Find the hard drive size
hdfs dfs -df -h

-- Copy the archive.zip file to the remote server
scp C:/Users/aporwal/Downloads/CSU_Learning_Journey/5200/Projects/Health_Insurance_Analysis/archive.zip aporwal@129.153.66.218:~

-- SSH into the remote server
ssh aporwal@129.153.66.218

-- Unzip the archive.zip file
unzip archive.zip

-- Create directories in HDFS
hdfs dfs -mkdir project
hdfs dfs -mkdir project/RawData
hdfs dfs -mkdir project/CleanedData
hdfs dfs -mkdir project/CleanedData/Benefitscostsharing
hdfs dfs -mkdir project/CleanedData/Network
hdfs dfs -mkdir project/CleanedData/Rate
hdfs dfs -mkdir project/CleanedData/Plan

-- List the contents of the project directory in HDFS
hdfs dfs -ls project/

-- Put the CSV files into the RawData directory in HDFS
hdfs dfs -put BenefitsCostSharing.csv project/RawData/
hdfs dfs -put PlanAttributes.csv project/RawData/
hdfs dfs -put Network.csv project/RawData/
hdfs dfs -put Rate.csv project/RawData/

-- List the contents of the RawData directory in HDFS
hdfs dfs -ls project/RawData

-- Count the number of rows in the following files
wc -l BenefitsCostSharing.csv
wc -l PlanAttributes.csv
wc -l Network.csv
wc -l Rate.csv

-- Count the number of columns in the BenefitsCostSharing.csv file
head -n 1 BenefitsCostSharing.csv | tr ',' '\n' | wc -l

-- List the header row or show header row name in the BenefitsCostSharing.csv file
head -n 1 BenefitsCostSharing.csv

-- Count the number of columns in the PlanAttributes.csv file
head -n 1 PlanAttributes.csv | tr ',' '\n' | wc -l

-- List the header row or show header row name in the PlanAttributes.csv file
head -n 1 PlanAttributes.csv

-- Count the number of columns in the Network.csv file
head -n 1 Network.csv | tr ',' '\n' | wc -l

-- List the header row or show header row name in the Network.csv file
head -n 1 Network.csv

-- Count the number of columns in the Rate.csv file
head -n 1 Rate.csv | tr ',' '\n' | wc -l

-- List the header row or show header row name in the Rate.csv file
head -n 1 Rate.csv

-- Change the permissions of the project directory in HDFS to be readable, writable, and executable by all users
hdfs dfs -chmod -R og+rwx /user/aporwal/project/

-- Open Beeline for Hive CLI
beeline;


CREATE DATABASE IF NOT EXISTS aporwal;

-- Show the list of databases
SHOW DATABASES;

-- Use the "aporwal" database
USE aporwal;

-- Drop the "Raw_Benefits" table if it exists
DROP TABLE IF EXISTS Raw_Benefits;


--Create external table for Raw_Benefits
CREATE EXTERNAL TABLE IF NOT EXISTS Raw_Benefits(
BenefitName STRING, 
BusinessYear STRING, 
CoinsInnTier1 STRING, 
CoinsInnTier2 STRING, 
CoinsOutofNet STRING, 
CopayInnTier1 STRING, 
CopayInnTier2 STRING, 
CopayOutofNet STRING, 
EHBVarReason STRING, 
Exclusions STRING, 
Explanation STRING, 
ImportDate STRING, 
IsCovered STRING, 
IsEHB STRING, 
IsExclFromInnMOOP STRING, 
IsExclFromOonMOOP STRING, 
IsStateMandate STRING, 
IsSubjToDedTier1 STRING, 
IsSubjToDedTier2 STRING, 
IssuerId STRING, 
IssuerId2 STRING, 
LimitQty STRING, 
LimitUnit STRING, 
MinimumStay STRING, 
PlanId STRING, 
QuantLimitOnSvc STRING, 
RowNumber STRING, 
SourceName STRING, 
StandardComponentId STRING, 
StateCode STRING, 
StateCode2 STRING, 
VersionNum STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
    'separatorChar' = ',', 
    'quoteChar'     = '"', 
    'escapeChar'    = '\\' 
) 
STORED AS TEXTFILE 
TBLPROPERTIES ('skip.header.line.count'='1'); 



-- This code loads data from the 'BenefitsCostSharing.csv' file into the 'Raw_Benefits' table.
-- The existing data in the 'Raw_Benefits' table will be overwritten.
LOAD DATA INPATH '/user/aporwal/project/RawData/BenefitsCostSharing.csv'  
OVERWRITE INTO TABLE Raw_Benefits; 

-- After loading the data, it displays the list of tables and shows the first 10 rows from the 'Raw_Benefits' table.
show tables; 
SELECT * FROM Raw_Benefits LIMIT 10;

-- This code creates the 'Cleaned_Benefits' table.
CREATE EXTERNAL TABLE Cleaned_Benefits( 
BenefitName STRING, 
BusinessYear INT, 
EHBVarReason STRING, 
Exclusions STRING, 
Explanation STRING, 
ImportDate DATE, 
IsCovered BOOLEAN, 
IsEHB BOOLEAN, 
IsStateMandate BOOLEAN, 
IsSubjToDedTier1 BOOLEAN, 
IssuerId INT, 
LimitQty INT, 
LimitUnit STRING, 
PlanId STRING, 
RowNumber INT, 
SourceName STRING, 
StandardComponentId STRING, 
StateCode STRING, 
VersionNum INT);   

-- This code inserts data into the 'Cleaned_Benefits' table from the 'Raw_Benefits' table.
INSERT INTO TABLE Cleaned_Benefits 
SELECT 
    BenefitName, 
    CAST(BusinessYear AS INT), 
    EHBVarReason, 
    Exclusions, 
    Explanation, 
    CAST(from_unixtime(unix_timestamp(ImportDate, 'yyyy-MM-dd')) as DATE) AS Importdate, 
    CAST(IsCovered AS BOOLEAN), 
    CAST(IsEHB AS BOOLEAN), 
    CAST(IsStateMandate AS BOOLEAN), 
    CAST(IsSubjToDedTier1 AS BOOLEAN), 
    CAST(IssuerId AS INT), 
    CAST(LimitQty AS INT), 
    LimitUnit, 
    PlanId, 
    CAST(RowNumber AS INT), 
    SourceName, 
    StandardComponentId, 
    StateCode, 
    CAST(VersionNum AS INT) 
FROM Raw_Benefits; 


-- Show the list of tables
show tables;

-- Describe the structure of the Cleaned_Benefits table
describe Cleaned_Benefits;

-- Create a view called Benefit_Count to calculate the unique benefit count per state
CREATE VIEW Benefit_Count AS 
SELECT StateCode, COUNT(DISTINCT BenefitName) AS UniqueBenefitCount 
FROM Cleaned_Benefits 
GROUP BY StateCode;

-- Create a temporary directory in HDFS
hdfs dfs -mkdir project/temp;

-- Export the Benefit_Count data to a CSV file in the temporary directory
INSERT OVERWRITE DIRECTORY '/user/aporwal/project/temp/'  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  
SELECT * FROM Benefit_Count 
ORDER BY StateCode;

-- Display the last two lines of the exported CSV file
hdfs dfs -cat project/temp/000000_0 | tail -n 2

-- List the files in the temporary directory
hdfs dfs -ls ./project/temp/

-- Download the exported CSV file
hdfs dfs -get project/temp/000000_0 BenefitCount.csv

-- Display the last two lines of the downloaded CSV file
tail -n 2 BenefitCount.csv

-- Copy the CSV file to a remote server
scp aporwal@129.153.66.218:/home/aporwal/BenefitCount.csv .

  

--2nd view 
-- This code creates a view called 'top5Benefits' that selects the top 5 benefits for each year from the 'Cleaned_Benefits' table.

CREATE VIEW top5Benefits AS 
WITH RankedBenefitCounts AS ( 
    SELECT 
        BusinessYear, 
        BenefitName, 
        COUNT(*) AS BenefitCount, 
        RANK() OVER (PARTITION BY BusinessYear ORDER BY COUNT(*) DESC) AS BenefitRank 
    FROM 
        Cleaned_Benefits WHERE BusinessYear IS NOT NULL 
    GROUP BY 
        BusinessYear, BenefitName 
)
 
SELECT 
    BusinessYear, 
    BenefitName, 
    BenefitCount 
FROM 
    RankedBenefitCounts 
WHERE 
    BenefitRank <= 5; 

  

/*
    This code block inserts data into a directory in Hadoop Distributed File System (HDFS).
    The data is formatted as a CSV file with fields separated by a comma.
    The directory path is '/user/aporwal/project/temp/'.
*/
INSERT OVERWRITE DIRECTORY '/user/aporwal/project/temp/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','


--This SQL query selects all rows from the "top5Benefits" table and orders them by the "BusinessYear" column in ascending order.
SELECT * FROM top5Benefits 
ORDER BY BusinessYear;

  

-- This code retrieves the last two lines of the file '000000_0' located in the 'project/temp/' directory in HDFS.
hdfs dfs -cat project/temp/000000_0 | tail -n 2 

-- This command lists the files and directories in the 'project/temp/' directory in HDFS.
hdfs dfs -ls ./project/temp/ 

-- Download the file '000000_0' from HDFS and save it as 'top5Benefits.csv'.
hdfs dfs -get project/temp/000000_0 top5Benefits.csv 

-- Display the last two lines of the 'top5Benefits.csv' file.
tail -n 2 top5Benefits.csv 

-- Copy the 'top5Benefits.csv' file from the remote server at IP address 129.153.66.218 and save it in the current directory.
scp aporwal@129.153.66.218:/home/aporwal/top5Benefits.csv .

 

--Network File 
-- This script creates an external table called Raw_Network to store health insurance network data.
-- The table has the following columns: BusinessYear, StateCode, IssuerId, SourceName, VersionNum, ImportDate, IssuerId2, StateCode2, NetworkName, NetworkId, NetworkURL, RowNumber, MarketCoverage, and DentalOnlyPlan.
-- The data is stored in CSV format with a separator character of ',' and a quote character of '"'.
-- The table is stored as a text file and skips the first line (header) during data loading.

-- Create the Raw_Network table
CREATE EXTERNAL TABLE IF NOT EXISTS Raw_Network(
    BusinessYear STRING,
    StateCode STRING,
    IssuerId STRING,
    SourceName STRING,
    VersionNum STRING,
    ImportDate STRING,
    IssuerId2 STRING,
    StateCode2 STRING,
    NetworkName STRING,
    NetworkId STRING,
    NetworkURL STRING,
    RowNumber STRING,
    MarketCoverage STRING,
    DentalOnlyPlan STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar'     = '"',
   'escapeChar'    = '\\'
)
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

-- Load data into the Raw_Network table from the specified file path
LOAD DATA INPATH '/user/aporwal/project/RawData/Network.csv'
OVERWRITE INTO TABLE Raw_Network;

-- Show the list of tables in the current database
SHOW TABLES;

-- Retrieve the first 10 rows from the Raw_Network table
SELECT * FROM raw_network LIMIT 10;

-- Drop the Cleaned_Network table if it already exists
DROP TABLE IF EXISTS Cleaned_Network;

  

CREATE TABLE Cleaned_Network
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/zpatel6/HealthProject/Cleaned_network'
AS
SELECT
CAST(BusinessYear AS INT),
StateCode,
CAST(IssuerId AS INT),
SourceName,
CAST(VersionNum AS INT),
CAST(from_unixtime(unix_timestamp(ImportDate, 'yyyy-MM-dd')) as DATE) AS Importdate,
NetworkName,
NetworkId,
NetworkURL,
CAST(RowNumber AS INT)
FROM Raw_Network;


SELECT * FROM Cleaned_Network LIMIT 5;  


-- Plan attributes: 
-- Create raw_planattr from csv file - 

CREATE EXTERNAL TABLE raw_planAttr ( 
AVCalculatorOutputNumber INT, 
BeginPrimaryCareCostSharingAfterNumberOfVisits STRING, 
BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays STRING, 
BenefitPackageId STRING, 
BusinessYear INT, 
CSRVariationType STRING, 
ChildOnlyOffering STRING, 
ChildOnlyPlanId STRING, 
CompositeRatingOffered STRING, 
DEHBCombInnOonFamilyMOOP STRING, 
DEHBCombInnOonFamilyPerGroupMOOP STRING, 
DEHBCombInnOonFamilyPerPersonMOOP STRING, 
DEHBCombInnOonIndividualMOOP STRING, 
DEHBDedCombInnOonFamily STRING, 
DEHBDedCombInnOonFamilyPerGroup STRING, 
DEHBDedCombInnOonFamilyPerPerson STRING, 
DEHBDedCombInnOonIndividual STRING, 
DEHBDedInnTier1Coinsurance STRING, 
DEHBDedInnTier1Family STRING, 
DEHBDedInnTier1FamilyPerGroup STRING, 
DEHBDedInnTier1FamilyPerPerson STRING, 
DEHBDedInnTier1Individual STRING, 
DEHBDedInnTier2Coinsurance STRING, 
DEHBDedInnTier2Family STRING, 
DEHBDedInnTier2FamilyPerGroup STRING, 
DEHBDedInnTier2FamilyPerPerson STRING, 
DEHBDedInnTier2Individual STRING, 
DEHBDedOutOfNetFamily STRING, 
DEHBDedOutOfNetFamilyPerGroup STRING, 
DEHBDedOutOfNetFamilyPerPerson STRING, 
DEHBDedOutOfNetIndividual STRING, 
DEHBInnTier1FamilyMOOP STRING, 
DEHBInnTier1FamilyPerGroupMOOP STRING, 
DEHBInnTier1FamilyPerPersonMOOP STRING, 
DEHBInnTier1IndividualMOOP STRING, 
DEHBInnTier2FamilyMOOP STRING, 
DEHBInnTier2FamilyPerGroupMOOP STRING, 
DEHBInnTier2FamilyPerPersonMOOP STRING, 
DEHBInnTier2IndividualMOOP STRING, 
DEHBOutOfNetFamilyMOOP STRING, 
DEHBOutOfNetFamilyPerGroupMOOP STRING, 
DEHBOutOfNetFamilyPerPersonMOOP STRING, 
DEHBOutOfNetIndividualMOOP STRING, 
DentalOnlyPlan STRING, 
DiseaseManagementProgramsOffered STRING, 
EHBPediatricDentalApportionmentQuantity STRING, 
EHBPercentPremiumS4 STRING, 
EHBPercentTotalPremium STRING, 
FirstTierUtilization STRING, 
FormularyId STRING, 
FormularyURL STRING, 
HIOSProductId STRING, 
HPID STRING, 
HSAOrHRAEmployerContribution STRING, 
HSAOrHRAEmployerContributionAmount STRING, 
ImportDate STRING, 
IndianPlanVariationEstimatedAdvancedPaymentAmountPerEnrollee STRING, 
InpatientCopaymentMaximumDays STRING, 
IsGuaranteedRate STRING, 
IsHSAEligible STRING, 
IsNewPlan STRING, 
IsNoticeRequiredForPregnancy STRING, 
IsReferralRequiredForSpecialist STRING, 
IssuerActuarialValue STRING, 
IssuerId STRING, 
IssuerId2 STRING, 
MEHBCombInnOonFamilyMOOP STRING, 
MEHBCombInnOonFamilyPerGroupMOOP STRING, 
MEHBCombInnOonFamilyPerPersonMOOP STRING, 
MEHBCombInnOonIndividualMOOP STRING, 
MEHBDedCombInnOonFamily STRING, 
MEHBDedCombInnOonFamilyPerGroup STRING, 
MEHBDedCombInnOonFamilyPerPerson STRING, 
MEHBDedCombInnOonIndividual STRING, 
MEHBDedInnTier1Coinsurance STRING, 
MEHBDedInnTier1Family STRING, 
MEHBDedInnTier1FamilyPerGroup STRING, 
MEHBDedInnTier1FamilyPerPerson STRING, 
MEHBDedInnTier1Individual STRING, 
MEHBDedInnTier2Coinsurance STRING, 
MEHBDedInnTier2Family STRING, 
MEHBDedInnTier2FamilyPerGroup STRING, 
MEHBDedInnTier2FamilyPerPerson STRING, 
MEHBDedInnTier2Individual STRING, 
MEHBDedOutOfNetFamily STRING, 
MEHBDedOutOfNetFamilyPerGroup STRING, 
MEHBDedOutOfNetFamilyPerPerson STRING, 
MEHBDedOutOfNetIndividual STRING, 
MEHBInnTier1FamilyMOOP STRING, 
MEHBInnTier1FamilyPerGroupMOOP STRING, 
MEHBInnTier1FamilyPerPersonMOOP STRING, 
MEHBInnTier1IndividualMOOP STRING, 
MEHBInnTier2FamilyMOOP STRING, 
MEHBInnTier2FamilyPerGroupMOOP STRING, 
MEHBInnTier2FamilyPerPersonMOOP STRING, 
MEHBInnTier2IndividualMOOP STRING, 
MEHBOutOfNetFamilyMOOP STRING, 
MEHBOutOfNetFamilyPerGroupMOOP STRING, 
MEHBOutOfNetFamilyPerPersonMOOP STRING, 
MEHBOutOfNetIndividualMOOP STRING, 
MarketCoverage STRING, 
MedicalDrugDeductiblesIntegrated STRING, 
MedicalDrugMaximumOutofPocketIntegrated STRING, 
MetalLevel STRING, 
MultipleInNetworkTiers STRING, 
NationalNetwork STRING, 
NetworkId STRING, 
OutOfCountryCoverage STRING, 
OutOfCountryCoverageDescription STRING, 
OutOfServiceAreaCoverage STRING, 
OutOfServiceAreaCoverageDescription STRING, 
PlanBrochure STRING, 
PlanEffictiveDate STRING, 
PlanExpirationDate STRING, 
PlanId STRING, 
PlanLevelExclusions STRING, 
PlanMarketingName STRING, 
PlanType STRING, 
QHPNonQHPTypeId STRING, 
RowNumber STRING, 
SBCHavingDiabetesCoinsurance STRING, 
SBCHavingDiabetesCopayment STRING, 
SBCHavingDiabetesDeductible STRING, 
SBCHavingDiabetesLimit STRING, 
SBCHavingaBabyCoinsurance STRING, 
SBCHavingaBabyCopayment STRING, 
SBCHavingaBabyDeductible STRING, 
SBCHavingaBabyLimit STRING, 
SecondTierUtilization STRING, 
ServiceAreaId STRING, 
SourceName STRING, 
SpecialistRequiringReferral STRING, 
SpecialtyDrugMaximumCoinsurance STRING, 
StandardComponentId STRING, 
StateCode STRING, 
StateCode2 STRING, 
TEHBCombInnOonFamilyMOOP STRING, 
TEHBCombInnOonFamilyPerGroupMOOP STRING, 
TEHBCombInnOonFamilyPerPersonMOOP STRING, 
TEHBCombInnOonIndividualMOOP STRING, 
TEHBDedCombInnOonFamily STRING, 
TEHBDedCombInnOonFamilyPerGroup STRING, 
TEHBDedCombInnOonFamilyPerPerson STRING, 
TEHBDedCombInnOonIndividual STRING, 
TEHBDedInnTier1Coinsurance STRING, 
TEHBDedInnTier1Family STRING, 
TEHBDedInnTier1FamilyPerGroup STRING, 
TEHBDedInnTier1FamilyPerPerson STRING, 
TEHBDedInnTier1Individual STRING, 
TEHBDedInnTier2Coinsurance STRING, 
TEHBDedInnTier2Family STRING, 
TEHBDedInnTier2FamilyPerGroup STRING, 
TEHBDedInnTier2FamilyPerPerson STRING, 
TEHBDedInnTier2Individual STRING, 
TEHBDedOutOfNetFamily STRING, 
TEHBDedOutOfNetFamilyPerGroup STRING, 
TEHBDedOutOfNetFamilyPerPerson STRING, 
TEHBDedOutOfNetIndividual STRING, 
TEHBInnTier1FamilyMOOP STRING, 
TEHBInnTier1FamilyPerGroupMOOP STRING, 
TEHBInnTier1FamilyPerPersonMOOP STRING, 
TEHBInnTier1IndividualMOOP STRING, 
TEHBInnTier2FamilyMOOP STRING, 
TEHBInnTier2FamilyPerGroupMOOP STRING, 
TEHBInnTier2FamilyPerPersonMOOP STRING, 
TEHBInnTier2IndividualMOOP STRING, 
TEHBOutOfNetFamilyMOOP STRING, 
TEHBOutOfNetFamilyPerGroupMOOP STRING, 
TEHBOutOfNetFamilyPerPersonMOOP STRING, 
TEHBOutOfNetIndividualMOOP STRING, 
TIN STRING, 
URLForEnrollmentPayment STRING, 
URLForSummaryofBenefitsCoverage STRING, 
UniquePlanDesign STRING, 
VersionNum STRING, 
WellnessProgramOffered STRING 
) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES (  
    'separatorChar' = ',',  
    'quoteChar'     = '"',  
    'escapeChar'    = '\\'  
)  
STORED AS TEXTFILE  
TBLPROPERTIES ('skip.header.line.count'='1'); 

-- Cleaned_planAttr table: 
CREATE EXTERNAL TABLE Cleaned_planAttr1 ( 
BusinessYear INT, 
DiseaseManagementProgramsOffered STRING, 
IssuerId INT, 
MarketCoverage STRING, 
PlanMarketingName STRING, 
PlanType STRING, 
StateCode STRING, 
NetworkId STRING, 
DentalOnlyPlan STRING, 
StandardComponentId STRING, 
PlanId STRING, 
PlanLevelExclusions STRING 
); 

--Insert in cleaned table: 

INSERT OVERWRITE TABLE Cleaned_planAttr 
SELECT  
BusinessYear, 
DiseaseManagementProgramsOffered, 
IssuerId, 
MarketCoverage, 
PlanMarketingName, 
PlanType, 
StateCode, 
NetworkId, 
DentalOnlyPlan, 
StandardComponentId, 
PlanId, 
PlanLevelExclusions 
FROM  
raw_planAttr; 


-- Join benefits and planattributes tables: 

CREATE TABLE tempNetBenefit 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES (  
   'separatorChar' = ',',  
   'quoteChar'     = '"',  
   'escapeChar'    = '\\'  
) 
STORED AS TEXTFILE  
LOCATION '/user/kbhanda3/tempNetBenefit'  
AS 
SELECT  
    b.businessyear, 
    b.BenefitName, 
    b.statecode, 
    b.issuerid, 
    b.planid, 
    p.DentalOnlyPlan, 
    p.marketcoverage, 
    p.networkid, 
    p.plantype 
FROM cleaned_benefits b 
LEFT OUTER JOIN raw_planattr p 
    ON b.businessyear = p.businessyear 
    AND b.IssuerID = p.IssuerID 
    AND b.statecode = p.statecode 
    AND b.planid = p.planid 
GROUP BY b.BusinessYear, b.Statecode, b.planid, p.PlanType, b.issuerid, b.BenefitName, p.DentalOnlyPlan, p.marketcoverage, p.networkid; 


-- Check if data exists: 
select businessyear,benefitname,statecode,planid,plantype from tempNetBenefit order by businessyear limit 20; 


-- Join tempNetBenefit and Network table to get list of network providers: 

CREATE TABLE tempNetPlanBenefit 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES (  
   'separatorChar' = ',',  
   'quoteChar'     = '"',  
   'escapeChar'    = '\\'  
) 
STORED AS TEXTFILE  
LOCATION '/user/kbhanda3/proj5200/tempNetPlanBenefit'  
AS 
SELECT  
    b.businessyear, 
    b.BenefitName, 
    b.statecode, 
    b.issuerid, 
    b.planid, 
    b.DentalOnlyPlan, 
    b.marketcoverage, 
    b.networkid, 
    b.plantype, 
	n.networkname 
FROM tempNetBenefit b 
LEFT OUTER JOIN  raw_network n 
    ON b.businessyear = n.businessyear 
    AND b.IssuerID = n.IssuerID 
    AND b.statecode = n.statecode 
    AND b.networkid = n.networkid; 

-- Check if data exists: 
SELECT businessyear,benefitname,statecode,plantype,networkname FROM tempNetPlanBenefit ORDER BY businessyear LIMIT 20;  

-- Query to show top 3 network providers across year: 

INSERT OVERWRITE DIRECTORY '/user/kbhanda3/proj5200/temp1/'   
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   
SELECT businessyear, statecode, networkname, count_of_values  
FROM ( 
    SELECT businessyear, statecode, networkname, COUNT(networkname) AS count_of_values, 
    ROW_NUMBER() OVER (PARTITION BY businessyear ORDER BY COUNT(REGEXP_REPLACE(networkname, '[^a-zA-Z0-9\\s]', '')) ASC) AS row_num  
    FROM tempNetPlanBenefit 
    WHERE networkname != plantype 
    GROUP BY businessyear, networkname, statecode 
) AS counts_per_year  
WHERE row_num IN (1,2,3); 

 

-- Get this data in our local system for analysis: 

hdfs dfs -ls proj5200/temp1 

-- Merge into single file & download it in Linux:
hdfs dfs -getmerge proj5200/temp1 temp1merge.csv 

-- Download the file to local system
scp kbhanda3@129.153.66.218:/home/kbhanda3/temp1merge.csv temp1merge.csv
 



-- Create temporary table to store network plan data
CREATE TABLE tempNetworkPlan 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  
LOCATION '/user/kbhanda3/proj5200/tempNetworkPlan'  
AS  
SELECT p.BusinessYear, p.Statecode, n.NetworkName, p.NetworkId, p.IssuerID, p.PlanType 
FROM cleaned_planAttr p  
LEFT JOIN cleaned_network n  
    ON n.IssuerId = p.IssuerId  
    AND n.Statecode = p.Statecode  
    AND n.Businessyear = p.Businessyear  
    AND n.networkid = p.networkid; 


-- Create network plan table
CREATE TABLE network_plan 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  
STORED AS TEXTFILE LOCATION '/user/zpatel6/Project/network_plan'  
AS  
SELECT p.BusinessYear, p.Importdate, p.IssuerId, p.NetworkId, p.PlanID, p.PlanType, p.StateCode, n.NetworkName, n.NetworkURL 
FROM cleaned_plan p LEFT OUTER JOIN cleaned_network n ON ( p.IssuerId = n.IssuerId AND p.Statecode = n.Statecode AND p.BusinessYear = n.BusinessYear AND p.networkid = n.networkid); 

-- Drop existing view if exists
DROP VIEW IF EXISTS totalnetworkplan; 

-- Create view to calculate total network count per plan type
CREATE VIEW totalnetworkplan 
AS 
SELECT plantype, COUNT(DISTINCT REGEXP_REPLACE(networkname, '[^a-zA-Z0-9\\s]', '')) AS totalnetworkcount 
FROM network_plan 
WHERE regexp_replace(networkname, '[^a-zA-Z0-9\\s]', '') IS NOT NULL 
GROUP BY plantype 
ORDER BY totalnetworkcount DESC; 

-- Retrieve data from totalnetworkplan view
SELECT * FROM totalnetworkplan; 

-- Create temporary directory
hdfs dfs -mkdir project/temp 

-- Export totalnetworkplan data to a file in the temporary directory
INSERT OVERWRITE DIRECTORY '/user/zpatel6/project/temp/'  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  
SELECT * FROM totalnetworkplan 
ORDER BY totalnetworkcount DESC; 

-- Display the last 2 lines of the file
hdfs dfs -cat project/temp/000000_0 | tail -n 2 

-- List files in the temporary directory
hdfs dfs -ls ./project/temp/ 

-- Download the file from HDFS to local machine
hdfs dfs -get project/temp/000000_0 networkcountplantype.csv  
tail -n 2 networkcountplantype.csv  

scp zpatel6@129.153.66.218:/home/zpatel6/networkcountplantype.csv . 

-- Drop existing view if exists
DROP VIEW IF EXISTS PlantypeState; 

-- Create view to calculate plan count per state and plan type
CREATE VIEW PlantypeState 
AS 
SELECT statecode, plantype, COUNT(*) as plancount 
FROM network_plan 
WHERE plantype IS NOT NULL
GROUP BY statecode, plantype 
ORDER BY statecode; 

-- Retrieve data from PlantypeState view
SELECT * FROM PlantypeState LIMIT 10; 

-- Export PlantypeState data to a file in the temporary directory
INSERT OVERWRITE DIRECTORY '/user/zpatel6/project/temp/'   
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   
SELECT * FROM PlantypeState 
ORDER BY statecode; 

-- Display the last 2 lines of the file
hdfs dfs -cat project/temp/000000_0 | tail -n 2 

-- List files in the temporary directory
hdfs dfs -ls ./project/temp/ 

-- Download the file from HDFS to local machine
hdfs dfs -get project/temp/000000_0 plantypestate.csv 
tail -n 2 plantypestate.csv 

scp zpatel6@129.153.66.218:/home/zpatel6/plantypestate.csv . 

CREATE EXTERNAL TABLE IF NOT EXISTS Rate( 
BusinessYear STRING, 
StateCode STRING, 
IssuerId STRING, 
SourceName STRING, 
VersionNum STRING, 
ImportDate STRING, 
IssuerId2 STRING, 
FederalTIN STRING, 
RateEffectiveDate STRING, 
RateExpirationDate STRING, 
PlanId STRING, 
RatingAreaId STRING, 
Tobacco STRING, 
Age STRING, 
IndividualRate STRING, 
IndividualTobaccoRate STRING, 
Couple STRING, 
PrimarySubscriberAndOneDependent STRING, 
PrimarySubscriberAndTwoDependents STRING, 
PrimarySubscriberAndThreeOrMoreDependents STRING, 
CoupleAndOneDependent STRING, 
CoupleAndTwoDependents STRING, 
CoupleAndThreeOrMoreDependents STRING, 
RowNumber STRING 
)  
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'  
WITH SERDEPROPERTIES (  
   'separatorChar' = ',',  
   'quoteChar'     = '"',  
   'escapeChar'    = '\\' 
) 
STORED AS TEXTFILE  
TBLPROPERTIES ('skip.header.line.count'='1');  

LOAD DATA INPATH '/user/clin22/project/RawData/Rate.csv'   
OVERWRITE INTO TABLE Rate; 

dDROP TABLE IF EXISTS Cleaned_Rate;  

CREATE TABLE Cleaned_Rate  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   
STORED AS TEXTFILE LOCATION '/user/clin22/project/CleanedData/Rate'   
AS   
SELECT 
CAST(BusinessYear AS INT), 
StateCode, 
CAST(IssuerId AS INT), 
SourceName, 
CAST(VersionNum AS INT), 
CAST(from_unixtime(unix_timestamp(ImportDate, 'yyyy-MM-dd')) as DATE) AS Importdate, 
CAST(IssuerId2 AS INT), 
FederalTIN, 
CAST(from_unixtime(unix_timestamp(RateEffectiveDate, 'yyyy-MM-dd')) as DATE) AS RateEffectiveDate, 
CAST(from_unixtime(unix_timestamp(RateExpirationDate, 'yyyy-MM-dd')) as DATE) AS RateExpirationDate, 
PlanId, 
RatingAreaId, 
Tobacco, 
Age, 
CAST(IndividualRate AS DECIMAL), 
IndividualTobaccoRate, 
CAST(Couple AS DECIMAL(7,2)), 
CAST(PrimarySubscriberAndOneDependent AS DECIMAL(7,2)), 
CAST(PrimarySubscriberAndTwoDependents AS DECIMAL(7,2)), 
CAST(PrimarySubscriberAndThreeOrMoreDependents AS DECIMAL(7,2)), 
CAST(CoupleAndOneDependent AS DECIMAL(7,2)), 
CAST(CoupleAndTwoDependents AS DECIMAL(7,2)), 
CAST(CoupleAndThreeOrMoreDependents AS DECIMAL(7,2)), 
CAST(RowNumber AS INT) 
FROM Rate; 

-- Drop the Network_Rate view if it already exists
drop view if exists Network_Rate;

-- Create the Network_Rate view
CREATE VIEW Network_Rate AS  
SELECT CK.BusinessYear AS NETWORK_YEAR, CK.StateCode AS NETWORK_STATE, CK.NetworkName AS NETWORK_NAME, CR.* 
FROM Cleaned_Network CK INNER JOIN Cleaned_Rate CR 
ON CK.IssuerID = CR.IssuerID;

-- Create a temporary directory in HDFS
hdfs dfs -mkdir project/temp

-- Export aggregated data from Network_Rate view to a file in the temporary directory
INSERT OVERWRITE DIRECTORY '/user/clin22/project/temp/'   
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   
SELECT BUSINESSYEAR, AVG(INDIVIDUALRATE) as INDIVIDUAL, AVG(COUPLE) AS COUPLE, 
AVG(primarysubscriberandonedependent) AS DEPENDENT, AVG(primarysubscriberandtwodependents) AS DEPENDENT2, AVG(primarysubscriberandthreeormoredependents) AS DEPENDENT3, 
AVG(coupleandonedependent) CDEPENDENT, AVG(coupleandtwodependents) CDEPENDENT2, AVG(coupleandthreeormoredependents) AS CDEPENDENT3 
FROM Network_Rate 
WHERE INDIVIDUALRATE IS NOT NULL AND COUPLE IS NOT NULL 
GROUP BY BUSINESSYEAR 
ORDER BY BUSINESSYEAR;

-- Display the last 2 lines of the exported file
hdfs dfs -cat project/temp/000000_0 | tail -n 2

-- List files in the temporary directory
hdfs dfs -ls ./project/temp/

-- Download the exported file from HDFS to local machine
hdfs dfs -get project/temp/000000_0 Cleaned_Rate.csv

-- Copy the exported file to another machine using SCP
scp clin22@129.153.66.218:/home/clin22/Cleaned_Rate.csv .
