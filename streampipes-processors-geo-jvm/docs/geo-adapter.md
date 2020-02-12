<header>
Geo Adapter Sources
===
</header>


# Introduction

Here you can find a list of geo-event stream.


## Sources for geo-events
* ISS live monitoring: http://api.open-notify.org/iss-now.json
* more to come


### Setup ISS Live Adapter

* Click on *Streampipes Connect*  choose REST HTTP Stream
   <p align="center">
    <img src="pics/stream_type.png" width="150px;" class="pe-image-documentation"/>

* In *Protocol settings* add the ISS live monitoring url, choose an interval e.g. 1 second.  and press next.
   <p align="center">
   <img src="pics/stream_settings.png" width="80%;" class="pe-image-documentation"/>

* Choose  Json Object and click next.
* Setup the Source:
  * Move the longitude and latitude values via drag & drop out of the *iss_position* folder. Afterwards delete the iss_position folder.
     <p align="center">
     <img src="pics/stream_source.png" width="60%;" class="pe-image-documentation"/>

  *  Modify the longitude value setup by clicking the *pencil-button* and change the *Runtime Type* from *String* to *Float*. You can also add infos for *Label* and *Description*.
     <p align="center">
     <img src="pics/field_setup_lng.png" width="60%;" class="pe-image-documentation"/>

  * Modify the latitude value the same way.

  * Modify the timestamp by clicking the *pen-button* and activate *Mark as timestamp* and choose *timestamp converter (unix timestamp)*  
     <p align="center">
     <img src="pics/filed_setup_time.png" width="60%;" class="pe-image-documentation"/>

  * Click next and fill out the *Adapter settings* with a name, description. Also add an icon e.g. [raumstation.png](downloads/raumstation.png) from the download folder <sup>[1](#myfootnote1)</sup>
     <p align="center">
     <img src="pics/adapter_settings.png" width="80%;" class="pe-image-documentation"/>

  * Start the adapter and watch the live events coming in.


Alternative you can use the provided adapter template in the download folder.


<a name="myfootnote1">1</a>: "Icon made by Freepik perfect from www.flaticon.com"
