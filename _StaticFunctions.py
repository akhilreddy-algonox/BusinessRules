import Lib
import pandas as pd
import sys
# comment below two for local testing
#from ace_logger import Logging
#logging = Logging()

# uncomment these below lines for local testing
import logging 
logger=logging.getLogger() 
logger.setLevel(logging.DEBUG) 

__methods__ = [] # self is a BusinessRules Object
register_method = Lib.register_method(__methods__)

@register_method
def evaluate_static(self, function, parameters):
    if function == 'Assign':
        return self.doAssign(parameters)
    if function == 'AssignQ':
        return self.doAssignQ(parameters)
    if function == 'CompareKeyValue':
        return self.doCompareKeyValue(parameters)
    if function == 'GetLength':
        return self.doGetLength(parameters)
    if function == 'GetRange':
        return self.doGetRange(parameters)
    if function == 'Select':
        return self.doSelect(parameters)
    if function == 'Transform':
        return self.doTransform(parameters)
    if function == 'Count':
        return self.doCount(parameters)
    if function == 'Contains':
        return self.doContains(parameters)
    if function == 'Read':
        return self.doRead(parameters)
    if function == 'Filter':
        return self.doFilter(parameters)
    if function == 'TransformDF':
        return self.doTransformDF(parameters)
    if function == 'WhereClause':
        return self.doWhereClause(parameters)
    if function == 'WriteToCSV':
        return self.doWriteToCSV(parameters)
    if function == 'GetTruthValues':
        return self.GetTruthValues(parameters)
    if function == 'Split':
        return self.doSplit(parameters)

@register_method
def doGetLength(self, parameters):
    """Returns the lenght of the parameter value.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       'parameters': {'param':{'source':'input', 'value':5},
                      }
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    try:
        value = len(self.get_param_value(parameters['param']))
    except Exception as e:
        logging.error(e)
        logging.error(f"giving the defalut lenght 0")
        value = 0
    return value

@register_method
def doGetRange(self, parameters):
    """Returns the parameter value within the specific range.
    Args:
        parameters (dict): The source parameter and the range we have to take into. 
    eg:
       'parameters': {'value':{'source':'input', 'value':5},
                        'range':{'start_index': 0, 'end_index': 4}
                      }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Range is the python range kind of (exclusive of the end_index)
    """
    logging.info(f"parameters got are {parameters}")
    value = self.get_param_value(parameters['value'])
    range_ = parameters['range']
    start_index = range_['start_index']
    end_index = range_['end_index']
    try:
        return (value.str[start_index: end_index])
    except Exception as e:
        logging.error(f"some error in the range function")
        logging.error(e)
    return ""

@register_method
def doSelect(self, parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'select_column': 'highlight',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name_to_select = parameters['select_column']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        query += f"{lookup_column} == {compare_value} & "
    query = query.strip(' & ') # the final strip for the extra &
    result_df = master_df.query(query)

    # get the wanted column from the dataframe
    if not result_df.empty:
        try:
            return result_df[column_name_to_select][0] # just return the first value of the matches
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return ""

@register_method
def doTransform(self,parameters) :
    """Returns the evalated data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':[
            {'param':{'source':'input', 'value':5}},
            {'operator':'+'},
            {'param':{'source':'input', 'value':7}},
            {'operator':'-'},
            {'param':{'source':'input', 'value':1}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':3}}
        ]
    Note:
        1) Recursive evaluations of rules can be made.
    """
    equation = ''
    logging.info(f"parameters got are {parameters}")
    for dic in parameters :
        for element,number_operator in dic.items() :
            if element == 'param' :
                value = f'{self.get_param_value(number_operator)}'
            elif element == 'operator' :
                value = f' {number_operator} '
        equation = equation + value
    print("in transaform $$$$$$$$$$$$$$$$$$$")
    print(equation)

    return(eval(equation))

@register_method
def doContains(self,parameters):
    """ Returns true value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            cpt_check_rule = {'rule_type': 'static',
                'function': 'Contains',
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            }
    """

    logging.info(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    value = self.get_param_value(parameters['value'])
    print(value,table_name,column_name)
    print(self.data_source['data'])
    if value in list(self.data_source[table_name][column_name]):
        print(table_name,column_name)
        return True
    else :
        return False

@register_method
def doCount(self, parameters):
    """Returns the count of records from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        query += f"{lookup_column} == {compare_value} & "
    query = query.strip(' & ') # the final strip for the extra &
    result_df = master_df.query(query)

    # get the wanted column from the dataframe
    if not result_df.empty:
        try:
            return len(result_df) # just return the first value of the matches
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return 0
    else:
        return 0

@register_method
def doRead(self,parameters):
    """Reads the csv file from the filepath and stores into the data source
    """
    logging.info(f"\n got the parameters {parameters}\n")
    try:
        df = pd.read_csv(self.get_param_value(parameters['path']))
        table_name = self.get_param_value(parameters['table_name'])
    except Exception as e:
        logging.error("\nError in reading the file\n")
        logging.error(e)
        df = pd.DataFrame({})
    
    self.data_source[table_name] = df
    
    return df

@register_method
def doFilter1(self, parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'select_column': 'highlight',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = self.get_param_value(parameters['from_table'])
    #column_name_to_select = parameters['select_column']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    """try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}"""

    #master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        operator_value = lookup['operator']
        ocr2 = self.data_source['ocr']
        query += f"({ocr2}['{lookup_column}'] == {compare_value}) {operator_value}"
    query = query.strip(f" {operator_value} ") # the final strip for the extra &
    print(query)
    self.data_source[from_table] = self.data_source[from_table][query]
    # get the wanted column from the dataframe
    """if not self.data_source[from_table].empty:
        try:
            return self.data_source[from_table] # just return the first value of the matches
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return e"""


######################################### 05-10-2019 #######################################
@register_method
def GetTruthValues(self,parameters):
    lookup_filters = parameters['lookup_filters']
    from_table = parameters['from_table']
    t_value = pd.Series([True]*len(self.data_source[from_table]))
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        lookup_operator = lookup['lookup_operator']
        compare_value = self.get_param_value(lookup['compare_with'])

        # our own ==
        if lookup_operator == '==':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] == compare_value))

        # our own ==
        if lookup_operator == '!=':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] != compare_value))
        
        # our own ==
        if lookup_operator == '>':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] != compare_value))
        
        # our own ==
        if lookup_operator == '<':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] != compare_value))
            
        # our own ==
        if lookup_operator == '>=':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] != compare_value))
            
        # our own ==
        if lookup_operator == '<=':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] != compare_value))
    
    return t_value

@register_method
def doTransformDF(self,parameters) :
    """Returns the evalated data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':[
            {'param':{'source':'input', 'value':5}},
            {'operator':'+'},
            {'param':{'source':'input', 'value':7}},
            {'operator':'-'},
            {'param':{'source':'input', 'value':1}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':3}},
            {'operator':'broadcast'},
            {'param':{'source':'input', 'value':Value}}
        ]
    Note:
        1) Recursive evaluations of rules can be made.
    """
    logging.info(f"parameters got are {parameters}")
    value1_colomn = self.get_param_value(parameters['value1_colomn'])
    value2 = self.get_param_value(parameters['value2'])
    operator_ = parameters['operator']
    table = parameters['table']
    
    try:
        if operator_ == "*":
            return (self.data_source[table][value1_colomn] * float(value2))
        if operator_ == "+":
            return (self.data_source[table][value1_colomn] + float(value2))
        if operator_ == "-":
            return (self.data_source[table][value1_colomn] - float(value2))
        if operator_ == "/":
            return (self.data_source[table][value1_colomn] / float(value2))
        if operator_ == "broadcast":
            return pd.Series([value2]*len(self.data_source[table]))
        
    except Exception as e:
        logging.error("\n error in transform function \n")
        logging.error(e)

@register_method
def doFilter(self, parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'lookup_operator' : '==',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'lookup_operator' : '==',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    lookup_filters = parameters['lookup_filters']
    t_value = pd.Series([True]*len(self.data_source[from_table]))
    print(t_value)
    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        lookup_operator = lookup['lookup_operator']
        compare_value = self.get_param_value(lookup['compare_with'])

        # our own ==
        if lookup_operator == '==':
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] == compare_value))
        # our own ==
        if lookup_operator == '!=':
            print(self.data_source['ocr']['Amount'] == 1000)
            t_value = (t_value & ((self.data_source[from_table])[lookup_column] != compare_value))

    try:
        self.data_source[from_table] = self.data_source[from_table][t_value]
        return True
    except Exception as e:
        logging.error("Couldn't update data after filtering")
        logging.error(e)
        return False

@register_method
def doWhereClause(self,parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'data_frame':{'source':'rule', 'value': Amount_Debit},
            'from_table': 'ocr',
            'column':'Amount',
            'lookup_filters':[
                {
                    'column_name': 'Plan Code',
                    'lookup_operator' : '==',
                    'compare_with':  {'source':'input', 'value': 'GP'}
                }
                
            ]
        }
    }"""

    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    t_value = self.get_param_value(parameters['t_value'])
    data_frame = self.get_param_value(parameters['data_frame'])
    column = parameters['colomn']
    try:
        return data_frame.where(t_value,self.data_source[from_table][column])
    except Exception as e:
        logging.error(e)
        logging.error("\n error in where doWhereClause")
    return

@register_method
def doSplit(self,parameters):
    df1 = self.get_param_value(parameters['table'])
    df1['Scheme Code'] = df1['Scheme Code'].str.split('-').str[0]
    print(df1)

@register_method
def doWriteToCSV(df, file_path, required_standard_mapping):
   """Write the dataframe to csv"""
   try:

       # first find whether any csv exists with the file_name
       existing_df = pd.read_csv(file_path)

       mode = 'a'
       headers = False
       df.to_csv(file_path, columns=required_columns, mode=mode, header=headers, index=False)
   except:
       # no sample file exists
       headers = True
       mode = 'w'
       
       # rename the columns in the dataframe to standardized columns            
       df = df.rename(columns=required_standard_mapping)
       
       required_columns = required_standard_mapping.values()

       df.to_csv(file_path, columns=required_columns, mode=mode, header=headers, index=False)
               
   return "Written to csv successfully"