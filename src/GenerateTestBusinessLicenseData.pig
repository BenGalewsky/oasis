-- TODO: Add Pig content here

IMPORT 'BusinessLicenseReader.pig';

--L  = BusinessLicenseReader('data/businessLicense.csv', 'data/Grocery_Stores_-_2013.csv');
L  = BusinessLicenseReader('data/testBusinesses/*', 'data/Grocery_Stores_-_2013.csv');

testL = FILTER L by (LICENSE_DESCRIPTION matches '.*Grocery.*'); -- AND ZIP_CODE == 60610;
STORE testL INTO 'output/testData' USING PigStorage(',');

