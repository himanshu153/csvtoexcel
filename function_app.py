import logging
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from io import BytesIO

app = func.FunctionApp()

# Azure Storage connection string
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=newstrgaccount;AccountKey=sD0MTh/zwZSxfvSGFZOFO0LVxJkZveBTDAqJ7jagmjexoiT6iF0clyZQq7x8/ZIBc52abHHlvwcF+ASt+VrQUA==;EndpointSuffix=core.windows.net"

@app.function_name(name="CsvToExcelFunction")
@app.blob_trigger(arg_name="myblob", path="newstrgaccount/input/{name}.csv", connection="AzureWebJobsStorage")
@app.blob_output(arg_name="outputBlob", path="newstrgaccount/output/{name}.xlsx", connection="AzureWebJobsStorage")
def main(myblob: func.InputStream, outputBlob: func.Out[func.InputStream]):
    logging.info(f"Processing blob: {myblob.name}")

    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    # Read the CSV file from the input blob
    csv_data = myblob.read().decode('utf-8')
    df = pd.read_csv(BytesIO(csv_data.encode()))

    # Automatically infer better data types for columns
    df = df.convert_dtypes()

    # Convert the DataFrame to an Excel file
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')

    # Set the output blob
    output.seek(0)
    outputBlob.set(output)

    # Upload the Excel file to the output container
    output_container_name = "newstrgaccount/output"
    output_blob_name = myblob.name.replace('.csv', '.xlsx')
    output_blob_client = blob_service_client.get_blob_client(container=output_container_name, blob=output_blob_name)
    output_blob_client.upload_blob(output, overwrite=True)


@app.route(route="http_trigger", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )