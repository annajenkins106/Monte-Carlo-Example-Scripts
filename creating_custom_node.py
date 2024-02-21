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

# Add your Monte Carlo API key (x-mcd-id, x-mcd-token)
x_mcd_id = "mcd_key_id" #replace with your own values
x_mcd_token = "mcd_secret" #replace with your own values

#create client call
client = Client(transport=RequestsHTTPTransport(
   url='https://api.getmontecarlo.com/graphql',
   headers={"x-mcd-id": x_mcd_id, "x-mcd-token": x_mcd_token}))

# This query pulls your DWH UUID which will be used throughout
## this script as a value passed in API call parameters.


query_1 = gql(
   """
   query {
     getUser{
       account{
         warehouses{
           name
           uuid
         }
         bi {
           uuid
         }
       }
     }
   }


"""
)


# Execute the query on the transport
result_1 = client.execute(query_1)
print(result_1)

#Variables for the query
assetName = "asset_name" #replace this value with the target asset name - e.g. table name
objectType = "table" #replace this value with the target asset object type - this is typically a table.

## this script as a value passed in API call parameters.
query_1 = gql(
     Template(
   """
query Search{
 search(
   limit:100,
   objectTypes:["$objectType"],
   query:"$assetName",
   offset:0
 ) {
   results {
     displayName
     objectId
     objectType
     resourceId
   }
   totalHits
 }
}


"""
 ).substitute(
   assetName = assetName,
   objectType = objectType
 )
)


# Execute the query on the transport
result_1 = client.execute(query_1)
print(result_1)


#populate query variables
name = "(ThoughtSpot) Dashboard" #name for new lineage node - this will be displayed in the lineage
objectId = "schema_name:dashboard" #object ID for new lineage node (includes schema)
objectType = "custom-bi-report" #object types can be many values (e.g. table, view, tableau-dashboard), please refer to our docs for full list
resourceId = "warehouse uuid" #this is the warehouse UUID where you want the node to be created


#you can also optionally add extra information about this node
propertyName1 = "Owner" #this is an example property name
propertyValue1 = "email" #this is an example property value
propertyName2 = "Description" #this is an example property name
propertyValue2 = "description of asset" #this is an example property value


#Build you query
create_custom_object_query = gql(
   Template(
       """
           mutation{
             createOrUpdateLineageNode(
               name: "$name"
               objectId: "$objectId"
               objectType: "$objectType"
               resourceId: "$resourceId"
               properties: [
                   {
                   propertyName: "$propertyName1",
                   propertyValue: "$propertyValue1"
                   },
                   {
                   propertyName: "$propertyName2",
                   propertyValue: "$propertyValue2"
                   }
               ]
             ){
               node{
                 displayName
                 objectType
                 mcon
               }
             }
           }
       """
   ).substitute(
     resourceId = resourceId,
     name = name,
     objectId = objectId,
     objectType = objectType,
     propertyName1 = propertyName1,
     propertyValue1 = propertyValue1,
     propertyName2 = propertyName2,
     propertyValue2 = propertyValue2
   )
)


#Execute the Query
create_custom_object_query_result = client.execute(create_custom_object_query)
print(json.dumps(create_custom_object_query_result,indent=4))

#populate query variables
existingObjectId = "schema:table_name" #this is the objectId of the existing asset
existingObjectType = "table" #type of the object you are connecting to
newObjectId = "schema_name:dashboard" #name for new node - should match what you created earlier
newObjectType = "custom-bi-report" #objectType of the newly created node - must match what you created
resourceId = "warehouse_uuid" #warehouse UUID for the lineage node
expireAt = "2099-09-16T00:00:00" # this defines when the lineage will expire - as in be removed. There are use cases where you want temporary lineage.

#Build you query
create_custom_edge_query = gql(
   Template(
       """
           mutation{
             createOrUpdateLineageEdge(
               source: {
                 objectId: "$existingObjectId"
                 objectType: "$existingObjectType"
                 resourceId: "$resourceId"
               }
               destination: {
                 objectId: "$newObjectId"
                 objectType: "$newObjectType"
                 resourceId: "$resourceId"
               }
               expireAt: "$expireAt"
             ){
               edge{
                 isCustom
                 expireAt
                 source {
                   displayName
                   mcon
                 }
                 destination {
                     displayName
                     mcon
                 }
               }
             }
           }
       """
   ).substitute(
     existingObjectId = existingObjectId,
     existingObjectType = existingObjectType,
     newObjectId = newObjectId,
     newObjectType = newObjectType,
     resourceId = resourceId,
     expireAt = expireAt)
)


#execute your query
create_custom_edge_result = client.execute(create_custom_edge_query)
print(json.dumps(create_custom_edge_result,indent=4))
