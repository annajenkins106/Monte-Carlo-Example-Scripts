!pip install gql
!pip install pycarlo
!pip install requests_toolbelt

# Get gql, a GraphQL package for Python, if you don't already have it:
# https://github.com/graphql-python/gql

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
from string import Template
import json
from collections import Counter
import pycarlo

from pycarlo.core import Client, Query, Mutation, Session

#First, use getAllUserDefinedMonitors to pull a list of monitors for, in this case, a single table.
#In general though, use getMonitors or getAllUserDefinedMonitors - whatever you need for the situation

def getMonitors(mcdId, mcdToken, table):
    client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
    query=Query()
    monQuery = """
        query {
            getAllUserDefinedMonitorsV2 ( userDefinedMonitorTypes: "custom_sql", entities: \"""" + table + """\")
            { 
                edges {
                    node {
                        monitorType
                        isDeleted
                        isSnoozed
                        lastRun
                        uuid
                        ruleName
                    }
                }
            }
        }
        """
    response = client(monQuery)
    return response['getAllUserDefinedMonitorsV2']['edges']

#Then trigger the group of monitors using triggerCustomRule or triggerMonitor (again, depends on the situation)

def runMonitor(mcdId,mcdToken,Id):
    client=Client(session=Session(mcd_id=mcdId,mcd_token=mcdToken))
    query=Query()
    monQuery = """
        mutation {
            triggerCustomRule(
                ruleId: \"""" + Id + """\")
                {
                    customRule{
                        ruleName
                }
            }
        }
    """
    response = client(monQuery)
    return response['triggerCustomRule']['customRule']


if __name__ == '__main__':
    #-------------------INPUT VARIABLES---------------------
    mcd_id = input("MCD ID: ")
    mcd_token = input("MCD Token: ")
    table = "demo_env:staging.salesforce_opportunity"#input ("Table: ")
    #-------------------------------------------------------

    if mcd_id and mcd_token and table:
        monitors = getMonitors(mcd_id,mcd_token,table)
        for m in monitors:
            print(runMonitor(mcd_id,mcd_token, m['node']['uuid']))
