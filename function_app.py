import azure.functions as func
import logging
from services.blob_service import BlobService
import asyncio
import pandas as pd
from execution.queryexecution import Queryexecution
import os
from io import BytesIO
from datetime import datetime
from shared_code.utilities import Utilities
from errors import MyException
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)



async def upload_to_container_function(result_list,servicetype):

    try:
        current_dateTime=datetime.utcnow()
        Time_string=str(current_dateTime.strftime("%Y"))+str(current_dateTime.strftime("%m"))+str(current_dateTime.strftime("%d"))
        for elem in result_list:
            blobobj=BlobService(
                storageaccount_endpoint=os.getenv('storage_endpoint'),
                conn_str=os.getenv('conn_str')
            )
            file_xlsx=BytesIO()
            df_temp=pd.DataFrame()
            df_temp=pd.DataFrame(elem['result'])
            with pd.ExcelWriter(file_xlsx,engine='openpyxl') as writer:
                df_temp.to_excel(writer,sheet_name=f"{servicetype}config",index=False)
            file_xlsx.seek(0)
            upload_file=await blobobj.upload_blob_to_container(
                data=file_xlsx,
                file_name=f"{servicetype}_"+elem['cid']+"_"+Time_string+".xlsx",
                container_name=elem['container_name']
            )
            logging.info(f"Status of upload file {upload_file}")
        return True
    except Exception as e:
        logging.info(f"Failure with uploading data to blob {e}")
        return e
    finally:
        file_xlsx.close()
        
@app.route(route="http_trigger")
async def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    querynsg="resources"\
            "| where type =~ 'microsoft.network/networksecuritygroups'"\
            "| mv-expand rules=properties.securityRules"\
            "| extend direction = tostring(rules.properties.direction)"\
            "| extend priority = toint(rules.properties.priority)"\
            "| extend description = rules.properties.description"\
            "| extend destprefix = rules.properties.destinationAddressPrefix"\
            "| extend destport = rules.properties.destinationPortRange"\
            "| extend sourceprefix = rules.properties.sourceAddressPrefix"\
            "| extend sourceport = rules.properties.sourcePortRange"\
            "| extend subnet_name = split(split(tostring(properties.subnets), '/')[-1],'\"}]')[0]" \
            "| where destprefix == '*'"\
            "| project subnet_name,name,tenantId,subscriptionId,direction,priority,destprefix,destport,sourceprefix,sourceport,description"

    queryrt="resources"\
            "| where type =~ 'Microsoft.Network/routeTables'"\
            "| mv-expand rules = properties.routes"\
            "| extend addressPrefix = tostring(rules.properties.addressPrefix)"\
            "| extend nextHopType = tostring(rules.properties.nextHopType)"\
            "| extend nextHopIpAddress = tostring(rules.properties.nextHopIpAddress)"\
            "| extend hasBgpOverride = tostring(rules.properties.hasBgpOverride)"\
            "| extend subnet_name = split(split(tostring(properties.subnets), '/')[-1],'\"}]')[0]"\
            "| extend udrname = rules.name"\
            "| project name,subnet_name,tenantId,subscriptionId,udrname, addressPrefix, nextHopType, nextHopIpAddress,hasBgpOverride"

    try:

        if os.getenv('httprun') == "nsg":
            servicetype="nsg"
            query=querynsg
        if os.getenv('httprun') == "rt":
            servicetype="rt"
            query=queryrt
        _get_query_result=await Queryexecution.query_result_function(query=query)
    except Exception as e:
        logging.error(f"Failed to fetch query from result")

    if _get_query_result is not None:
        for elem in _get_query_result:
            if elem['credential_key']=='CredSAPTenant':
                df_saptenant=pd.DataFrame(elem['result'])
            elif elem['credential_key']=='CredSharedTenant':
                df_sapshared=pd.DataFrame(elem['result'])
            elif elem['credential_key']=='CredChinaTenant':
                df_china=pd.DataFrame(elem['result'])
        result_xlsx=BytesIO()
        with pd.ExcelWriter(result_xlsx,engine='openpyxl') as writer:
            df_saptenant.to_excel(writer,sheet_name='SAP Tenant',index=False)
            df_sapshared.to_excel(writer,sheet_name='SHARD Tenant',index=False)
            df_china.to_excel(writer,sheet_name="China Tenant",index=False)
        
        result_xlsx.seek(0)

        try:
            obj=BlobService(
                storageaccount_endpoint=os.getenv('storage_endpoint'),
                conn_str=os.getenv('conn_str')
            )
            current_dateTime=datetime.utcnow()
            Time_string=str(current_dateTime.strftime("%Y"))+str(current_dateTime.strftime("%m"))+str(current_dateTime.strftime("%d"))
            await obj.upload_blob_to_container(
                data=result_xlsx,
                file_name=f"{servicetype.upper()}_"+Time_string+".xlsx",
                container_name=f"{servicetype}backupconfig"
            )
        except Exception as e:
            logging.error(f"Failed to upload http data {servicetype} {e}")
            
    return func.HttpResponse(
             str(_get_query_result),
             status_code=200
        )

@app.route(route="http_trigger_customer", auth_level=func.AuthLevel.FUNCTION)
async def http_trigger_customer(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    querynsg="resources"\
            "| where type =~ 'microsoft.network/networksecuritygroups'"\
            "| mv-expand rules=properties.securityRules"\
            "| extend direction = tostring(rules.properties.direction)"\
            "| extend priority = toint(rules.properties.priority)"\
            "| extend description = rules.properties.description"\
            "| extend destprefix = rules.properties.destinationAddressPrefix"\
            "| extend destport = rules.properties.destinationPortRange"\
            "| extend sourceprefix = rules.properties.sourceAddressPrefix"\
            "| extend sourceport = rules.properties.sourcePortRange"\
            "| extend subnet_name = split(split(tostring(properties.subnets), '/')[-1],'\"}]')[0]" \
            "| where destprefix == '*'"\
            "| project subnet_name,name,tenantId,subscriptionId,direction,priority,destprefix,destport,sourceprefix,sourceport,description"
    
    queryrt="resources"\
            "| where type =~ 'Microsoft.Network/routeTables'"\
            "| mv-expand rules = properties.routes"\
            "| extend addressPrefix = tostring(rules.properties.addressPrefix)"\
            "| extend nextHopType = tostring(rules.properties.nextHopType)"\
            "| extend nextHopIpAddress = tostring(rules.properties.nextHopIpAddress)"\
            "| extend hasBgpOverride = tostring(rules.properties.hasBgpOverride)"\
            "| extend subnet_name = split(split(tostring(properties.subnets), '/')[-1],'\"}]')[0]"\
            "| extend udrname = rules.name"\
            "| project name,subnet_name,tenantId,subscriptionId,udrname, addressPrefix, nextHopType, nextHopIpAddress,hasBgpOverride"
    
    obj2=BlobService(
        storageaccount_endpoint=os.getenv('storage_endpoint'),
        conn_str=os.getenv('conn_str')
    )
    custlist=await obj2.read_container_file(
        blob_name=os.getenv('custlistblobname'),
        container_name=os.getenv('custlistcontainername')
    )
    custlist_converted=BytesIO(custlist)
    df_custlist=pd.read_excel(custlist_converted,engine='openpyxl')
    df_custlist_dict=df_custlist.to_dict(orient='records')
    logging.info(f"Dict contents {df_custlist_dict}")
    try:
        cust_results_nsg = await asyncio.gather(
            *[
                Queryexecution.query_result_function(
                    query=querynsg,
                    tenantName=cust['tenantName'],
                    subid=cust['subid'],
                    cid=cust['cid'],
                    container_name=cust['container_name']
                )
                for cust in df_custlist_dict
            ]
        )
        cust_results_rt=await asyncio.gather(
            *[
                Queryexecution.query_result_function(
                    query=queryrt,
                    tenantName=cust['tenantName'],
                    subid=cust['subid'],
                    cid=cust['cid'],
                    container_name=cust['container_name']
                )
                for cust in df_custlist_dict
            ]
        )
    except Exception as e:
        logging.info(f"Error running with query for customer {e}")

    try:
        logging.info(f"Length of customer {cust_results_nsg[0]}")
        logging.info(f"Length of customer {cust_results_rt[0]}")
        if cust_results_nsg is not None:
            nsg_list=Utilities.flattened_function(cust_results_nsg)
            logging.info(f"NSG List Function {nsg_list}")
            await upload_to_container_function(nsg_list,servicetype="NSG")
        if cust_results_rt is not None:
            rt_list=Utilities.flattened_function(cust_results_rt)
            logging.info(f"NSG List Function {rt_list}")
            await upload_to_container_function(rt_list,servicetype="RT")
    except Exception as e:
        logging.error(f"Error with customer service {e}")
    
    
    return func.HttpResponse(
             str(cust_results_rt),
             status_code=200
        )

@app.timer_trigger(schedule="0 0 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
async def timer_trigger_nsg(myTimer: func.TimerRequest) -> None:
    
    querynsg="resources"\
            "| where type =~ 'microsoft.network/networksecuritygroups'"\
            "| mv-expand rules=properties.securityRules"\
            "| extend direction = tostring(rules.properties.direction)"\
            "| extend priority = toint(rules.properties.priority)"\
            "| extend description = rules.properties.description"\
            "| extend destprefix = rules.properties.destinationAddressPrefix"\
            "| extend destport = rules.properties.destinationPortRange"\
            "| extend sourceprefix = rules.properties.sourceAddressPrefix"\
            "| extend sourceport = rules.properties.sourcePortRange"\
            "| extend subnet_name = split(split(tostring(properties.subnets), '/')[-1],'\"}]')[0]" \
            "| where destprefix == '*'"\
            "| project subnet_name,name,tenantId,subscriptionId,direction,priority,destprefix,destport,sourceprefix,sourceport,description"
    

    try:
        _get_query_result=await Queryexecution.query_result_function(query=querynsg)
    except Exception as e:
        logging.error(f"Error with running query NSG{e}")
        MyException(e,name="Error with running query NSG")

    if _get_query_result is not None:
        for elem in _get_query_result:
            if elem['credential_key']=='CredSAPTenant':
                df_saptenant=pd.DataFrame(elem['result'])
            elif elem['credential_key']=='CredSharedTenant':
                df_sapshared=pd.DataFrame(elem['result'])
            elif elem['credential_key']=='CredChinaTenant':
                df_china=pd.DataFrame(elem['result'])
        result_xlsx=BytesIO()
        with pd.ExcelWriter(result_xlsx,engine='openpyxl') as writer:
            df_saptenant.to_excel(writer,sheet_name='SAP Tenant',index=False)
            df_sapshared.to_excel(writer,sheet_name='SHARD Tenant',index=False)
            df_china.to_excel(writer,sheet_name="China Tenant",index=False)
        
        result_xlsx.seek(0)
        try:

            obj=BlobService(
                storageaccount_endpoint=os.getenv('storage_endpoint'),
                conn_str=os.getenv('conn_str')
            )
            current_dateTime=datetime.utcnow()
            Time_string=str(current_dateTime.strftime("%Y"))+str(current_dateTime.strftime("%m"))+str(current_dateTime.strftime("%d"))
            await obj.upload_blob_to_container(
                data=result_xlsx,
                file_name=f"NSG_"+Time_string+".xlsx",
                container_name=f"nsgconfigbackup"
            )
        except Exception as e:
            logging.error(f"Error with execution {e}")
            MyException(e,name="Failed to take backup of NSG")



@app.timer_trigger(schedule="0 0 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
async def timer_trigger_rt(myTimer: func.TimerRequest) -> None:
    
    queryrt="resources"\
            "| where type =~ 'Microsoft.Network/routeTables'"\
            "| mv-expand rules = properties.routes"\
            "| extend addressPrefix = tostring(rules.properties.addressPrefix)"\
            "| extend nextHopType = tostring(rules.properties.nextHopType)"\
            "| extend nextHopIpAddress = tostring(rules.properties.nextHopIpAddress)"\
            "| extend hasBgpOverride = tostring(rules.properties.hasBgpOverride)"\
            "| extend subnet_name = split(split(tostring(properties.subnets), '/')[-1],'\"}]')[0]"\
            "| extend udrname = rules.name"\
            "| project name,subnet_name,tenantId,subscriptionId,udrname, addressPrefix, nextHopType, nextHopIpAddress,hasBgpOverride"
    
    try:
        _get_query_result=await Queryexecution.query_result_function(query=queryrt)
    except Exception as e:
        logging.error(f"Error with running query RT {e}")
        MyException(e,name="Error with running query RT")

    if _get_query_result is not None:
        for elem in _get_query_result:
            if elem['credential_key']=='CredSAPTenant':
                df_saptenant=pd.DataFrame(elem['result'])
            elif elem['credential_key']=='CredSharedTenant':
                df_sapshared=pd.DataFrame(elem['result'])
            elif elem['credential_key']=='CredChinaTenant':
                df_china=pd.DataFrame(elem['result'])
        result_xlsx=BytesIO()
        with pd.ExcelWriter(result_xlsx,engine='openpyxl') as writer:
            df_saptenant.to_excel(writer,sheet_name='SAP Tenant',index=False)
            df_sapshared.to_excel(writer,sheet_name='SHARD Tenant',index=False)
            df_china.to_excel(writer,sheet_name="China Tenant",index=False)
        
        result_xlsx.seek(0)
        try:
            obj=BlobService(
                storageaccount_endpoint=os.getenv('storage_endpoint'),
                conn_str=os.getenv('conn_str')
            )
            current_dateTime=datetime.utcnow()
            Time_string=str(current_dateTime.strftime("%Y"))+str(current_dateTime.strftime("%m"))+str(current_dateTime.strftime("%d"))
            await obj.upload_blob_to_container(
                data=result_xlsx,
                file_name=f"RT_"+Time_string+".xlsx",
                container_name=f"rtconfigbackup"
            )
        except Exception as e:
            logging.error(f"Error with execution {e}")
            MyException(e,name="Failed to take backup of RT")

@app.timer_trigger(schedule="0 0 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
async def timer_trigger_customer(myTimer: func.TimerRequest) -> None:
    
    querynsg="resources"\
            "| where type =~ 'microsoft.network/networksecuritygroups'"\
            "| mv-expand rules=properties.securityRules"\
            "| extend direction = tostring(rules.properties.direction)"\
            "| extend priority = toint(rules.properties.priority)"\
            "| extend description = rules.properties.description"\
            "| extend destprefix = rules.properties.destinationAddressPrefix"\
            "| extend destport = rules.properties.destinationPortRange"\
            "| extend sourceprefix = rules.properties.sourceAddressPrefix"\
            "| extend sourceport = rules.properties.sourcePortRange"\
            "| extend subnet_name = split(split(tostring(properties.subnets), '/')[-1],'\"}]')[0]" \
            "| where destprefix == '*'"\
            "| project subnet_name,name,tenantId,subscriptionId,direction,priority,destprefix,destport,sourceprefix,sourceport,description"
    
    queryrt="resources"\
            "| where type =~ 'Microsoft.Network/routeTables'"\
            "| mv-expand rules = properties.routes"\
            "| extend addressPrefix = tostring(rules.properties.addressPrefix)"\
            "| extend nextHopType = tostring(rules.properties.nextHopType)"\
            "| extend nextHopIpAddress = tostring(rules.properties.nextHopIpAddress)"\
            "| extend hasBgpOverride = tostring(rules.properties.hasBgpOverride)"\
            "| extend subnet_name = split(split(tostring(properties.subnets), '/')[-1],'\"}]')[0]"\
            "| extend udrname = rules.name"\
            "| project name,subnet_name,tenantId,subscriptionId,udrname, addressPrefix, nextHopType, nextHopIpAddress,hasBgpOverride"
    
    obj2=BlobService(
        storageaccount_endpoint=os.getenv('storage_endpoint'),
        conn_str=os.getenv('conn_str')
    )
    custlist=await obj2.read_container_file(
        blob_name=os.getenv('custlistblobname'),
        container_name=os.getenv('custlistcontainername')
    )
    custlist_converted=BytesIO(custlist)
    df_custlist=pd.read_excel(custlist_converted,engine='openpyxl')
    df_custlist_dict=df_custlist.to_dict(orient='records')
    logging.info(f"Dict contents {df_custlist_dict}")
    try:
        cust_results_nsg = await asyncio.gather(
            *[
                Queryexecution.query_result_function(
                    query=querynsg,
                    tenantName=cust['tenantName'],
                    subid=cust['subid'],
                    cid=cust['cid'],
                    container_name=cust['container_name']
                )
                for cust in df_custlist_dict
            ]
        )
        cust_results_rt=await asyncio.gather(
            *[
                Queryexecution.query_result_function(
                    query=queryrt,
                    tenantName=cust['tenantName'],
                    subid=cust['subid'],
                    cid=cust['cid'],
                    container_name=cust['container_name']
                )
                for cust in df_custlist_dict
            ]
        )
    except Exception as e:
        logging.error(f"Error executing query for customer {e}")
        MyException(e,name="Error executing query for customer")

    try:
        logging.info(f"Length of customer {cust_results_nsg[0]}")
        logging.info(f"Length of customer {cust_results_rt[0]}")
        if cust_results_nsg is not None:
            nsg_list=Utilities.flattened_function(cust_results_nsg)
            await upload_to_container_function(nsg_list,servicetype="NSG")
        if cust_results_rt is not None:
            rt_list=Utilities.flattened_function(cust_results_rt)
            await upload_to_container_function(rt_list,servicetype="RT")
    except Exception as e:
        logging.error(f"Error with customer service {e}")
        MyException(e,name="Failed to take backup of Customer")