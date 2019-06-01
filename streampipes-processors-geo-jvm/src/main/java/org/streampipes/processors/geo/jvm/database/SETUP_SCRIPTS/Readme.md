Please copy these scripts to another folder before executing due massive download files.
scripts are meant to be used on Linux.

additional package installation may be necessary!
especially for pgrouting

https://workshop.pgrouting.org/2.0.2/en/chapters/installation.html
sudo apt-get install osm2pgrouting  -> version 2.3.3 but  actual version is 2.6.1 on github
config files should be under /usr/share/osm2pgrouting/mapconfig.xml or in github
https://github.com/pgRouting/pgrouting

import from baden-württemberg takes around 5 hrs!!!!!!!!!!!!
and there is a problem that not all roads are imported. that leads to empty routing results.
workaround in progress and coming soon.

SRTM import can take a while as well due slow single download approach
for testing chose your region e.g. eurasia instead of downloading the whole world
Data is SRTM data with 9ßm resolution
30m would also be possible but without automatically download


geofence names are unique and if processor is normally closed, database entries will be removed automatically.
During debugging and hard shutdown names still exists and are blocked. 
If a name is blocked, geofence name will be extended with numbers 
e.g. geofencename is streampipes, next try is streampipes1, next try is streampipes12 and so on.
To clean up again, names has to be deleted in the geofence.main table manually.
trigger for workaround in progress (for long term data e.g. 1 day without update)

