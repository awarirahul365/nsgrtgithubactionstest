from services.auth_service import AuthService
from services.graph_service import GraphService
from services.subscription_service import SubscriptionService
import asyncio
import logging
from typing import Optional
class Queryexecution:

    @staticmethod
    async def runquery(cred:str,query:str,subid:Optional[str]=None,cid:Optional[str]=None,container_name:Optional[str]=None):
        credential,cloud=AuthService.get_credential(credential_key=cred)
        async with credential:
            if subid is not None:
                subscriptions=[subid]
                sub_ids=subscriptions
            else:
                subscriptions=await SubscriptionService.subscription_list(credentials=credential,cloud=cloud)
                sub_ids=SubscriptionService.filter_ids(subscriptions)
            qres=await GraphService.run_query(
                query_str=query,
                credential=credential,
                sub_ids=sub_ids,
                cloud=cloud
            )
        if subid is None:
            res_dict={
                'credential_key':cred,
                'result':qres
            }
        else:
            res_dict={
                'cid':cid,
                'result':qres,
                'container_name':container_name
            }
        return res_dict

    @staticmethod
    async def query_result_function(query:str,tenantName:Optional[str]=None,subid:Optional[str]=None,cid:Optional[str]=None,container_name:Optional[str]=None):
        try:
            if tenantName is None:
                credential_keys=AuthService.get_credential_keys()
            else:
                credential_keys=[tenantName]
            logging.info(f"credential_key {credential_keys}")
            query_result=await asyncio.gather(
                *(asyncio.create_task(
                    Queryexecution.runquery(cred,query,subid,cid,container_name)
                )for cred in credential_keys)
            )
            return query_result
        except Exception as e:
            logging.error(f"Error with query execution {e}")