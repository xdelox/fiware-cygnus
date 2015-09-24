# -*- coding: utf-8 -*-
#
# Copyright 2015 Telefonica Investigación y Desarrollo Chile
#
# This file is part of fiware-cygnus (FI-WARE project).
#
# fiware-cygnus is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
# Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
# later version.
# fiware-cygnus is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License along with fiware-cygnus. If not, see
# http://www.gnu.org/licenses/.
#
# For those usages not covered by the GNU Affero General Public License please contact:
#  iot_support at tid.es
#
__author__ = 'Herman Junge (herman dot junge at telefonica dot com)'

Feature: Store in postgresql new notifications per row from context broker
  As a cygnus user
  I want to be able to store in postgresql new notifications per row from context broker

  @happy_path
  Scenario Outline: stored new notifications in postgresql from context broker with or without metadata
    Given copy properties.json file from "epg_properties.json" to test "postgresql-sink" and sudo local "false"
    And configuration of cygnus instances with different ports "true", agents files quantity "1", id "test" and in "row" mode
    And copy flume-env.sh, grouping rules file from "grouping_rules.conf", log4j.properties, krb5.conf and restart cygnus service. This execution is only once "true"
    And verify if cygnus is installed correctly
    And verify if postgresql is installed correctly

