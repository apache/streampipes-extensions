#!/bin/bash
# version 1.0

echo Start SRTM download North America
wget wget -r --no-parent https://dds.cr.usgs.gov/srtm/version2_1/SRTM3/North_America/
echo Start unzipping North America
unzip 'dds.cr.usgs.gov/srtm/version2_1/SRTM3/North_America/*.zip'


echo Start SRTM download Africa
wget wget -r --no-parent https://dds.cr.usgs.gov/srtm/version2_1/SRTM3/Africa/
echo Start unzipping Africa
unzip 'dds.cr.usgs.gov/srtm/version2_1/SRTM3/Africa/*.zip'

echo Start SRTM download South America
wget wget -r --no-parent https://dds.cr.usgs.gov/srtm/version2_1/SRTM3/South_America/
echo Start unzipping South America
unzip 'dds.cr.usgs.gov/srtm/version2_1/SRTM3/South_America/*.zip'

echo Start SRTM download Eurasia
wget wget -r --no-parent https://dds.cr.usgs.gov/srtm/version2_1/SRTM3/Eurasia/
echo Start unzipping Eurasia
unzip 'dds.cr.usgs.gov/srtm/version2_1/SRTM3/Eurasia/*.zip'

echo Start SRTM download Australia
wget wget -r --no-parent https://dds.cr.usgs.gov/srtm/version2_1/SRTM3/Australia/
echo Start unzipping Australia
unzip 'dds.cr.usgs.gov/srtm/version2_1/SRTM3/Australia/*.zip'

raster2pgsql -C -I -F -t 500x500 *.hgt elevation.srtm90 | psql -h localhost -p 65434 -U streampipes -w -d streampipes

exit 0
 