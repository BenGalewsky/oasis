-- TODO: Add Pig content here

IMPORT 'BusinessLicenseReader.pig';

L = BusinessLicenseReader('data/businessLicense.csv');

testL = FILTER L by (LICENSE_DESCRIPTION matches '.*Food.*') AND ZIP_CODE == 60610;
STORE testL INTO 'output' USING PigStorage(',');

