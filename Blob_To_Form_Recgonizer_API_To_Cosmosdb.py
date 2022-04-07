# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 08:05:13 2022

@author: btindol
"""

"""
This code sample shows Prebuilt Read operations with the Azure Form Recognizer client library. 
The async versions of the samples require Python 3.6 or later.

To learn more, please visit the documentation - Quickstart: Form Recognizer Python client library SDKs v3.0
https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/quickstarts/try-v3-python-sdk
"""
# INSTALL THESE !!!! 
#pip install azure-ai-formrecognizer==3.2.0b3 # beta version get  DocumentAnalysisClient problem cant find function in library
#pip install azure-core
#pip install azure-storage-blob==0.37.1 
########################################################################################
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
import os
import json
import uuid

# pip install azure-storage-blob --upgrade

connect_str = "<Storage Account>" # retrieve the connection string from the environment variable

container_name = "/form-recognizer/forms" # container name in which images will be store in the storage account

blob_service_client = BlobServiceClient.from_connection_string(conn_str=connect_str) # create a blob service client to interact with the storage account

container_client = blob_service_client.get_container_client(container=container_name)

endpoint = "https://eastus.api.cognitive.microsoft.com/"
key = "<Storage Account Key>"

# Blob container
container = ContainerClient.from_connection_string(conn_str=connect_str, container_name="form-recognizer")#/upload-destination contract-intelligence-arrival

###########################################################################
# Commit this json object to cosmos db
from azure.cosmos import exceptions, CosmosClient, PartitionKey

#pip install --pre azure-cosmos
# Initialize the Cosmos client
endpointcbd = "<Cosmosdb Endpoint>"
keycbd = '<Cosmos db Key> '

# <create_cosmos_client>
client = CosmosClient(endpointcbd, keycbd)
###########################################################################

blob_list = container.list_blobs(name_starts_with="forms/")#container,prefix="forms/
for blob in blob_list:
    print(blob.name + '\n')
    blobname = blob.name
    newblobname = blobname.replace("forms/", "")
    print("New blob name is: ",newblobname)
    # Make string for formatting 
    unfinishedUrl = "https://<Storage Account Name>.blob.core.windows.net/form-recognizer/{blobname}" # Faster for development 1 page (WORKS!!)
    print("Unfinished url is: ",unfinishedUrl)

    # complete URL that specific blob
    formUrl = unfinishedUrl.format(blobname=blobname)
    print("Blob full URL is: ",formUrl)

    # This is the document analysis connection client
    document_analysis_client = DocumentAnalysisClient(endpoint=endpoint, credential=AzureKeyCredential(key))
                    
    # Grab that file and do the analysis 
    poller = document_analysis_client.begin_analyze_document_from_url("prebuilt-read", formUrl)

    # get the results 
    result = poller.result()

    print ("Document contains content: ", result.content)
    
    # go into the url in the blob where you want to find your uploaded pdf

    #  This works one page png from the PPOR1005.pdf
    document_analysis_client = DocumentAnalysisClient(endpoint=endpoint, credential=AzureKeyCredential(key))
        
    poller = document_analysis_client.begin_analyze_document_from_url("prebuilt-read", formUrl)
    result = poller.result()
    #print("THE RESULTS OF FORM RECOGNIZER API IS!: ",result)

    print ("Document contains content: ", result.content)
    
    # # Writing the results to a text file for later analysis send this to the blob!! 
    dateTimeObj = datetime.now();
    time = str(dateTimeObj.year) + '-'  + str(dateTimeObj.month)  + '-' +  str(dateTimeObj.day) +  '-'+ str(dateTimeObj.hour) +  '-' +  str(dateTimeObj.minute) +  '-' +  str(dateTimeObj.second)
    print("Time is: ",time)

    png_filename = blobname.replace("forms/", "") # remove the directory so the path does send file to another place
    print("With png name: ",png_filename) # to use for delete blob referal 

    # For json output contract name extraction (blob name extraction)
    contract_name_only = png_filename.replace('.png','')
    contract_name_only = contract_name_only.replace('.pdf','')

    print("CONTRACT NAME EXTRACTION IS!!! ",contract_name_only)
    contract_name_no_postfix = png_filename.replace(".png", ".txt") # save it to txt file so change .png or pdf to .txt 
    print("The blob name with txt is: ",contract_name_no_postfix)
    blob_name = contract_name_no_postfix.replace(".pdf", ".txt")
    print("New blob name is: ",blob_name)

    resultname = "CI_FR_{time}_{blob_name}" # CI (Contract intelligence) _ FR (Form Recognizer) Time stamp blob name 
    print("The preformat name is: ",resultname)

    filename = resultname.format(time=time,blob_name=blob_name)
    print("The final file name is: ",filename) 

    container_name = "form-recognizer/end-destination" #end-destination  or forms 

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    # Write the new file name to local txt file destination
    text_file = open(filename, "w")#,encoding="utf-8"
    n = text_file.write(result.content)
    blob_client.upload_blob(result.content)
    text_file.close()
    #blob_client.upload_blob(result.content)
    print("The file is ",text_file)
    print( "The txt file has been uploaded!!!")

    # Try to create a local directory to hold blob data if not already made
    try:
        local_path = "./data" # make new folder
        os.mkdir(local_path)
    except:
        pass


    ############################
    # Make json output
    ############################
    # ADD other things like business unit and such
    # UPLOAD DATE 
    dateTimeObj = datetime.now();
    time = str(dateTimeObj.year) + '-'  + str(dateTimeObj.month)  + '-' +  str(dateTimeObj.day) 
    print("Time is: ",time)
    
    # creation of a unique id 
    uniqueid = contract_name_only + "_"+ time + "_" + str(uuid.uuid4())

    contract_info = {}
    contract_info["id"] = uniqueid
    contract_info["contract_name"]=contract_name_only
    contract_info["contract_content"] = result.content 
    contract_info["upload_date"] = time
    print(json.dumps(contract_info))

    # Create a database
    # <create_database_if_not_exists>
    database_name = 'ContractIntelligenceDatabase'
    database = client.create_database_if_not_exists(id=database_name)
    # </create_database_if_not_exists>

    # Create a container
    # Using a good partition key improves the performance of database operations.
    # <create_container_if_not_exists>
    container_name = 'ContractContainer'
    containercbd = database.create_container_if_not_exists(
        id=container_name, 
        partition_key=PartitionKey(path="/contract_name"),
        offer_throughput=400)

    # Create item using the contract_info json data 
    containercbd.create_item(body=contract_info)
  
######################################################################
    import time
    time.sleep(10) # Allow the blob to show up in the folder 
######################################################################
    # QUERY ITEMS 

    # Query these items using the SQL query syntax. 
    # Specifying the partition key value in the query allows Cosmos DB to retrieve data only from the relevant partitions, which improves performance
    # <query_items>
    query = "SELECT * FROM c WHERE c.contract_name = 'PPOR1005_1'"

    items = list(containercbd.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    print("Items are: ",items)

    request_charge = containercbd.client_connection.last_response_headers['x-ms-request-charge']

    print('Query returned {0} items. Operation consumed {1} request units'.format(len(items), request_charge))
    # </query_items>









    

