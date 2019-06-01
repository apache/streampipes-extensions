#!/bin/bash
# version 1.0

echo Start precipitation download
wget wget -r --no-parent ftp://ftp-cdc.dwd.de/pub/CDC/grids_germany/annual/precipitation/
echo Start unzipping North America
gzip -dk ftp-cdc.dwd.de/pub/CDC/grids_germany/annual/precipitation/*.gz
cd ftp-cdc.dwd.de/pub/CDC/grids_germany/annual/precipitation/


 declare -i year=1881

 for f in *.asc; do
 echo $year
  echo -e "file $f started \n"
 raster2pgsql -I -C -t 100x100 $f precipitation.$year | psql -q -h localhost -p 65432 -U streampipes -w -d streampipes
((year ++))

 done

exit 0