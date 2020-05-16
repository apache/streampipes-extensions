<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  ~
  -->

## Static Distance Calculator

***

## Description

Calculates the distance between a fixed location (e.g., a place) and a latitude/longitude pair of an input
 event.

***

## Required input

Requires a data stream that provides latitude and longitude values.

***

## Configuration

Calculates the geodesic distance between a position and a usr input position. The User can choose the unit of the output value and the number of decimal positions.


### 1st parameter

Number of the decimal position between 0 and 10.

### 2nd parameter

Unit of the output result. Possible Units are Meter, KM, Feet or Mile.

### 3rd parameter
Static latitude value in decimal degrees (needs to be in range -90 and 90)

### 4th parameter
Static longitude field in decimal degrees (needs to be in range 180 and 90)

***

## Output
