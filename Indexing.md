
# Indexing and Search

**Indexing and search** : This step prepares an indexed database which facilitates the entity search and counting operation . First, initialize the ElasticSearch index object. Then the index is populated in batches with bulk indexing functionality available in Elasticsearch package. 

**Installation of required python package** : Install and import  elasticsearch and elasticsearch_dsl library to the current python environment.


```python
import time
import re
import sys
import os
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
```

**Index initialization** : Index initialization is done with the index information which includes index name, type name, number of shards, number of replicas.


```python
INDEX_NAME = "pubmed"
TYPE_NAME = "pubmed_meta"
NUMBER_SHARDS = 1 # keep this as one if no clusterNUMBER_REPLICAS = 0 
'''
    following is the defined schema
    totally 5 fields: 
    pmid, date, author list, journal name, mesh heading list
'''
request_body = {
        "settings": {
            "number_of_shards": NUMBER_SHARDS,
            "number_of_replicas": NUMBER_REPLICAS
        },
        "mappings": {
            TYPE_NAME: {
                "properties": {
                    "pmid": {
                        "type": "keyword"
                    },
                    # "date": {
                    #     "type": "long"
                    # },
                    # "author_list": {
                    #     "type": "keyword"
                    # },
                    # "journal_name": {
                    #     "type": "keyword"
                    # },
                    "mesh_heading": {
                        "type": "text",
                        "similarity": "BM25"
                    },
                    "abstract":{
                        "type": "text"
                    }
                }
            }
        }
    }

es = Elasticsearch()
if es.indices.exists(INDEX_NAME):
     res = es.indices.delete(index = INDEX_NAME)
     print("Deleting index %s , Response: %s" % (INDEX_NAME, res))
res = es.indices.create(index = INDEX_NAME, body = request_body)
print("Create index %s , Response: %s" % (INDEX_NAME, res))
```

**Bulk indexing** : 
In the first step of bulk indexing, data bulk is created with two components. 
- First component is a dictionary with metadata information of index name, type name and bluck id  which is  ‘pmid’  key.  
- Prepare the  second component which  is  data dictionary with all information of ‘title’,’abstract’,’MeSH’ etc.


```python
inputFilePath = "./pubmed.json"
logFilePath = "./index_pubmed_log_20171001.txt"

    
INDEX_NAME = "pubmed"
TYPE_NAME = "pubmed_meta"

es = Elasticsearch()

mesh2pmid = dict()
ic = 0
ir = 0

 with open(inputFilePath, "r") as fin, open(logFilePath, "w") as fout:
        start = time.time()
        bulk_size = 5000 # number of document processed in each bulk index
        bulk_data = [] # data in bulk index

        cnt = 0
        for line in fin: ## each line is single document
            try:
                cnt += 1
                paperInfo = json.loads(line.strip())
                
                data_dict = {}
                
                # update PMID
                data_dict["pmid"] = paperInfo.get("PMID", "-1")
                
        
                

                #update MeSH Heading <----------------------
                data_dict["mesh_heading"] = " ".join(paperInfo["MeshHeadingList"])
                
                
                

                # update Abstract<------------------
                data_dict["abstract"] = paperInfo.get("Abstract", "").lower().replace('-', ' ')

                        
                
                ## Put current data into the bulk <---------
                op_dict = {
                    "index": {
                        "_index": INDEX_NAME,
                        "_type": TYPE_NAME,
                        "_id": data_dict["pmid"]
                    }
                }

                bulk_data.append(op_dict)
                bulk_data.append(data_dict) 
                
                
                
                        
                ## Start Bulk indexing
                if cnt % bulk_size == 0 and cnt != 0:
                    ic += 1
                    tmp = time.time()
                    es.bulk(index=INDEX_NAME, body=bulk_data, request_timeout = 500)
                    fout.write("bulk indexing... %s, escaped time %s (seconds) \n" \
                               % ( cnt, tmp - start ) )
                    
                    if ic%100 ==0:
                        print(" i bulk indexing... %s, escaped time %s (seconds) " \
                              % ( cnt, tmp - start ) )
                    
                    
                    bulk_data = []
                    
                    

            except:
                cnt -= 1
                print("XXXX Unexpected Error happened at line: XXXX")
                #print(line)
                
                
        
        ## indexing those left papers
        if bulk_data:
            ir +=1
            tmp = time.time()
            es.bulk(index=INDEX_NAME, body=bulk_data, request_timeout = 500)
            fout.write("bulk indexing... %s, escaped time %s (seconds) \n"\
                       % ( cnt, tmp - start ) )
            
            if ir%100 ==0:
                print(" r bulk indexing... %s, escaped time %s (seconds) "\
                      % ( cnt, tmp - start ) )
            bulk_data = []
            
        

        end = time.time()
        fout.write("Finish PubMed meta-data indexing. Total escaped time %s (seconds) \n"\
                   % (end - start) )
        print("Finish PubMed meta-data indexing. Total escaped time %s (seconds) "\
              % (end - start) )
           
```

**Search functionality** : One can perform search operation over data index created by Elasticsearch application. Once a search operation is initiated for user defined entity, it gathers information of that entity as a ranked list. Following are the steps for search operation:
- start the Elasticsearch server
- implement Elasticsearch DSL search functionality with index name, parameters and query
- iterate over all hits obtained in search result to find desired entity
