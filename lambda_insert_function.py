import os
import json
import pymysql

##retrieving database credentials from environment variables
db_username = os.environ['db_username']
db_password = os.environ['db_password']
db_endpoint = os.environ['db_endpoint']
db_name = os.environ['db_name']


##code partially constructed from chatGPT because I didn't want to write that many variable names
def lambda_handler(event, context):
    # Extract data from the API request parameters
    try:
        # Assuming event contains the parameters sent by the API
        tif_name = event['tif_name']
        tif_year = event['tif_year']
        start_year = event['start_year']
        end_year = event['end_year']
        tif_number = event['tif_number']
        property_tax_extraction = event['property_tax_extraction']
        cumulative_property_tax_extraction = event['cumulative_property_tax_extraction']
        transfers_in = event['transfers_in']
        cumulative_transfers_in = event['cumulative_transfers_in']
        expenses = event['expenses']
        fund_balance_end = event['fund_balance_end']
        transfers_out = event['transfers_out']
        distribution = event['distribution']
        admin_costs = event['admin_costs']
        finance_costs = event['finance_costs']
        
        ##error handling for the "bank" variables which may be null or may not be null and in fact have a bank thingy in it
        bank = event.get('bank', None)
    except KeyError as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Missing parameter: {str(e)}')
        }
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Error parsing parameters: {str(e)}')
        }
    
    
    conn = pymysql.connect(host=db_endpoint, user=db_username, password=db_password, database=db_name)
    cursor = conn.cursor()
    
    ##this part also partially made with chatGPT because I also didn't want to write out that many variables
    try:
        # SQL INSERT statement
        sql = "INSERT INTO your_table_name (tif_name, tif_year, start_year, end_year, tif_number, property_tax_extraction, cumulative_property_tax_extraction, transfers_in, cumulative_transfers_in, expenses, fund_balance_end, transfers_out, distribution, admin_costs, finance_costs, bank) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        
        # Execute the INSERT statement
        cursor.execute(sql, (tif_name, tif_year, start_year, end_year, tif_number, property_tax_extraction, cumulative_property_tax_extraction, transfers_in, cumulative_transfers_in, expenses, fund_balance_end, transfers_out, distribution, admin_costs, finance_costs, bank))
        
        # Commit the transaction
        conn.commit()
        
        # Return a success message
        response = {
            'statusCode': 200,
            'body': json.dumps('Data inserted successfully')
        }
    except Exception as e:
        ##emergency rollback in case of error
        conn.rollback()
        
        ##returning error
        response = {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    finally:
        cursor.close()
        conn.close()

    return response