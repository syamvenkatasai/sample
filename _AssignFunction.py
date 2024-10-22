# please dont change this file without talking with the Author
try:
    import Lib
except:
    from . import Lib
import json


__methods__ = [] # self is a BusinessRules Object
register_method = Lib.register_method(__methods__)

try:
    from ace_logger import Logging
    logging = Logging()
except:
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 

import json
import os
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}


@register_method
def doAssign(self, parameters):
    """Update the parameter in the data_source of this class.
    Also update the changed fields that we are keeping track of.

    Args:
        parameters (dict): the left parameters where to assign and right one what to assign
    eg:
       'parameters': {'assign_place':{'source':'input_config', table':'ocr', 'column':invoice_no},
                       'assign_value':{'source':'input', 'value':4}
                    }
    Note:
        1) Recursive evaluations of rules can be made.
        eg:
            'parameters': {'assign_table':{'source':'input', 'value':5},
                       'assign_value':{'source':'rule', 'value':rule}
                    }
                value is itself can be a rule.
    """
    logging.debug(f"##### Assign parameters got are {parameters}")
    table_key = parameters.get('table',"")
    column_key = parameters.get('column',"")
    assign_value = self.get_data(parameters)

    try:
        logging.info(f"Updated the data source with the values {table_key} {column_key}\n ")
        self.data_source[table_key][column_key] = assign_value
    except Exception as e:
        logging.error(f"Couldnt update the data source with the values")
        logging.error(e)
        
    # update the changed fields
    try:
        if table_key not in self.changed_fields:
            self.changed_fields[table_key] = {}
        self.changed_fields[table_key][column_key] = assign_value
        logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        return True
    except Exception as e:
        logging.error(f"error in assigning and updating the fields")
        logging.error(e)
    return False


# required db_utils for stats thing.
# from db_utils import DB

@register_method
def doAssignQ(self, parameters):
    """Update the parameter in the data_source of this class.
    Also update the changed fields that we are keeping track of.
    
    
    Actually modifications are being done in the database ...as we know the queue that is being 
    getting assigned ...
    
    We have to think about the updations in the database....Design changes might be required....

    Also as of now we are keeping the assign_placd so that there are less changes in the rule_strings
    and also backward compatable....
    
    Actually its not required.

    Args:
        parameters (dict): the left parameters where to assign and right one what to assign
    eg:
       'parameters': {'assign_place':{'source':'input_config', table':'ocr', 'column':invoice_no},
                       'assign_value':{'source':'input', 'value':4}
                    }
    Note:
        1) Recursive evaluations of rules can be made.
        eg:
            'parameters': {'assign_table':{'source':'input', 'value':5},
                       'assign_value':{'source':'rule', 'value':rule}
                    }
                value is itself can be a rule.
    """
    logging.debug(f"parameters got are {parameters}")
    # assign_table = parameters['assign_table']
    assign_value = self.get_data(parameters)

    table_key = parameters['table']
    column_key = parameters['column']
    # update the data source if the value exists
    try:
        logging.info(f"Updated the data source with the values {table_key} {column_key}\n ")
        self.data_source[table_key][column_key] = assign_value
    except Exception as e:
        logging.error(f"Couldnt update the data source with the values")
        logging.error(e)
        
    # update the changed fields
    try:
        if table_key not in self.changed_fields:
            self.changed_fields[table_key] = {}
        self.changed_fields[table_key][column_key] = assign_value
        logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        return True
    except Exception as e:
        logging.error(f"error in assigning and updating the fields")
        logging.error(e)

    return False
        
       
        
        
    # try inserting in the audit data.....db update because...its queue update....
    # improvements can be done..once we use generic insert and update functions that are being used
    # in the db_utils ...
    # try:
    #     stats_db_config = {
    #         'host': 'stats_db',
    #         'user': 'root',
    #         'password': 'root',
    #         'port': '3306'
    #     }
    #     stats_db = DB('stats', tenant_id = self.tenant_id, **db_config)
    #     audit_data = {
    #                     "type": "update", "last_modified_by": "business_rules", "table_name": "process_queue", "reference_column": "case_id",
    #                     "reference_value": self.case_id, "changed_data": json.dumps({"queue":assign_value})
    #                 }
    #     #stats_db.insert_dict(audit_data, 'audit') 
    # except Exception as e:
    #     logging.error("error in inserting into audit data for queues")
    #     logging.error(e)
    # return False

@register_method
def doAssignTable(self, parameters):
    """ Assigns value to column in a inner table
    "parameters":{
        'table_name':"Table",
        'column_name':"RATE",
        'assign_value':{'source':'table', 'table_name':'Table','column_name':'DUTY'}
        
    }
    """
    logging.debug(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    row_index = self.index
    assign_value = self.get_data(parameters)
    try:
        try:
            table_data = json.loads(self.tables[table_name])
        except:
            table_data = self.tables[table_name]
    except Exception as e:
        logging.error("Extracting of Table Data Failed")
        logging.error(e)
        table_data = []
    try:
        columns_list = [col_data[0] for col_data in table_data[0]]
        print (columns_list)
        column_index = columns_list.index(column_name)
        table_data[row_index][column_index][0] = assign_value
        self.tables[table_name] = table_data
        return True
    except Exception as e:
        logging.error("Failed in Assining value to table data")
        logging.error(e)
        return False




