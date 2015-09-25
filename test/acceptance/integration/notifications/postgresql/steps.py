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

from integration.notifications.common_steps.multi_instances import * # common_steps to multi-instances
from integration.notifications.common_steps.configuration import *   # common_steps to pre-configurations
from integration.notifications.common_steps.notifications import *   # common_steps to notifications
from integration.notifications.common_steps.grouping_rules import *   # common_steps to grouping rules


@step(u'verify if postgresql is installed correctly')
def postgresql_is_installed_correctly(step):
    """
     verify that PostgreSQL is installed correctly, version is controlled
    :param step:
    """
    world.postgresql.connect()
    world.postgresql.verify_version()

@step(u'Close postgresql connection')
def close_postgresql_connection(step):
    """
    Close postgresql connection
    :param step:
    """
    world.cygnus.close_connection()

@step (u'create a new database and a table with attribute data type "([^"]*)" and metadata data type "([^"]*)"')
def create_a_new_table_with_service_attributes_attribute_type_attribute_data_type_and_metadata_data_type (step, attribute_data_type, metadata_data_type):
    """
     create a new Database and a new table per column mode
    :param step:

    :param attribute_data_type:
    :param metadata_data_type:
    """
    world.cygnus.create_database()
    world.cygnus.create_table (attribute_data_type, metadata_data_type)

@step (u'Verify that the attribute value is stored in postgresql')
def verify_that_the_attribute_value_is_stored_in_postgresql(step):
    """
    Validate that the attribute value and type are stored in postresql per column
    :param step:
    """
    world.cygnus.verify_table_search_values_by_column()

@step (u'Verify the metadatas are stored in postgresql')
def verify_the_metadatas_are_stored_in_postgresql(step):
    """
    Validate that the attribute metadata is stored in postgresql per column
    :param step:
    """
    world.cygnus.verify_table_search_metadatas_values_by_column()

@step (u'Verify that is not stored in postgresql "([^"]*)"')
def verify_that_is_not_stored_in_postgresql(step, error_msg):
    """
    Verify that is not stored in postgresql
    :param step:
    :param error_msg:
    """
    world.cygnus.verify_table_search_without_data (error_msg)

@step (u'Validate that the attribute value, metadata "([^"]*)" and type are stored in postgresql')
def validate_that_the_attribute_value_and_type_are_stored_in_postgresql(step, metadata):
    """
    Validate that the attributes values and type are stored in postgresql per row mode
    :param step:
    """
    world.cygnus.verify_table_search_values_by_row(metadata)
