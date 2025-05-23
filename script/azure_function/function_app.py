import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import azure.functions as func
import logging
from script.main_task import main_task
from script.write_log import write_log
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="etl")
def etl(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    start_row_index, end_row_index, source = req.params.get('start_row_index'), req.params.get('end_row_index'), req.params.get('source'),
    write_log(start_row_index, end_row_index, source, status='started')
    main_task(start_row_index, end_row_index)
    result = main_task(start_row_index, end_row_index)
    log = write_log(start_row_index, end_row_index, source, result.get('status', 'failed'))
    if start_row_index and end_row_index:
        if result:
            # Write
            return func.HttpResponse(log)
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a start_row_index and end_row_index in the query string or in the request body for a personalized response.",
             status_code=200
        )
