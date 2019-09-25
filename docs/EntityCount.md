
# Entity Count

 CaseOLAP score calculation requires the entity count per document. This step provides the data which connects metadata for text-cube structure and entity-count per document.
 
**Selection of entity** : Entity(phrase) are defined by user to be studied under the text-cube document structure. User defined entity could be protein names, chemicals, disease or signs and symptoms etc.
Search and listing of entity based document: The search functionality in  Elasticsearch DSL package uses index name, parameters and query to list the document from indexed database. The query includes all representatives of the specific entity e.g. synonyms, abbreviations. 

**Entity count** : Then each document from the list is analysed one by one to count the entity also called term frequency, ```tf(p,c)``` . With the help of text-cube metadata, for each of the document in cell of the text-cube, the entity count is recorded as PMID to entity count mapping.

**Text-cube metadata update** : Once the entity count is completed, text cube metadata is updated by adding PMID to entity count mapping . Following are the additional metadata prepared:
- count the total occurance of each entity ```cntP(c)```  within each cell,
- count the total number of documents ```df(p,c)``` within a cell in which entity appears.
- calculate normalized term frequency  ```ntf(p,c)``` [eq ref]  and normalized document frequency  ndf(p,c) [eq ref] using quantities obtained above.




#### Import required libearies


```python
import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from collections import Counter

year_constraints = None
```

#### Setting up input output directories


```python
input_index_dir = "../../elasticsearch-6.2.2/data"
input_file_pmid_and_cat = "input/pmid_and_cat.json" # a file containing cell to pmid mapping
input_file_entity_list = "../data/entities.txt"  # a file containing all entities
output_file_paper_entity_count = "output/paper_entity_count.txt"# an output file for entity count per PMID
output_file_paper_category = "output/paper_category.txt" # an output file containing PMID to cell mapping
```

#### Read in paper info and entity info


```python
"""
Read in paper info and entity info
"""
with open(input_file_pmid_and_cat, "r") as f_in:
    pmid_and_cat = json.load(f_in)
concerned_pmid_set = set(map(lambda x: x[0], pmid_and_cat))

entity_count_per_pmid = {pmid: Counter() for pmid in concerned_pmid_set}


entity_dict = {}
with open(input_file_entity_list, "r") as f_in:
    for line in f_in:
        # synonums seperated by "|" and represented by the first one on each line
        line_split = line.strip().split("|")
        entity_dict[line_split[0]] = line_split

```

#### Search and count entities: to optimize and find count from indexer


```python
"""
Search and count entities: to optimize and find count from indexer
"""
es = Elasticsearch(timeout=300)
k = 0
for entity_rep in entity_dict:
    for entity in entity_dict[entity_rep]:
        
        
        #entity_space_sep = "".join(map(lambda x: " " if x == "_" else x, entity))
        entity_space_sep = entity.replace("_", " ")
        
        
        
        # s = Search(using=es, index="pmc_all_index").query("match", abstract=entity_space_sep)
        s = Search(using=es, index="pubmed")\
                    .params(request_timeout=300)\
                    .query("match_phrase", abstract=entity_space_sep)
                

        num_hits = 0
        num_valid_hits = 0
        num_counts = 0
        
        for hit in s.scan():
            num_hits += 1
            cur_pmid = str(hit.pmid)
            if cur_pmid not in concerned_pmid_set:
                continue
                
            #if hit.PMCflag != 0:
            #    continue
            if year_constraints is not None:
                # to-do
                print("To add year constraint handler.")

                
            abs_lower = hit.abstract.lower().replace("-", " ")
            entity_lower = entity_space_sep.lower().replace("-", " ")
            entity_cnt = abs_lower.count(entity_lower)

            
            if entity_cnt == 0:
                #print "----------", entity_space_sep, "----------"
                #print abs_lower
                continue

                
            entity_count_per_pmid[cur_pmid][entity_rep] += entity_cnt
            num_valid_hits += 1
            num_counts += entity_cnt

        #print(entity, "# hits:", num_hits, "# valid hits:", num_valid_hits, "# counts:", num_counts)
    k = k +1
    if k%1000 == 0:
        print(k,'entity counted!')
            
```

#### Entity count metadata update in each Cell


```python
"""
Output
"""
## paper entity count & paper category
with open(output_file_paper_entity_count, "w") as f_out_entity_count,\
        open(output_file_paper_category, "w") as f_out_category:
    f_out_category.write("doc_id\tlabel_id\n")
    
    paper_new_id = 1
    
    for cur_pmid, cur_cat in pmid_and_cat:
        
        if len(entity_count_per_pmid[cur_pmid]) == 0:
            continue
            
            
        # print paper category
        f_out_category.write(str(cur_pmid) + "\t" + str(cur_cat) + "\n")
        
        
        # print paper entity count
        f_out_entity_count.write(str(cur_pmid))
        
        
        for entity in entity_count_per_pmid[cur_pmid]:
            f_out_entity_count.write(" " + entity +"|" + str(entity_count_per_pmid[cur_pmid][entity]))
            
            
        f_out_entity_count.write("\n")

        
        paper_new_id += 1

```

##### PMID to Entity count Dictionary


```python
with open('paper_entity_count.txt') as ff:
    pmid2pcount = {}
    PMID2PCOUNT = []
    for line in ff:
        item = line.split()
        pmid = item[0]
        if len(item)>1:
            prot_freq = {}
            for pf in item[1:]:
                pfs = pf.split('|')
                prot_freq.update({pfs[0]:pfs[1]})
                pmid2pcount.update({pmid:prot_freq})
with open('pmid2pcount.json', 'w') as fp:
                json.dump(pmid2pcount, fp)
```

#### PMID to Category Dictionary


```python
with open("paper_category.txt") as f:
    
    allpmids = []
    PMID2CVD = []
    cvd2pmids = {}
    dis0 = []
    dis1 = []
    for line in f:
        item = line.split()
        if item[0] != 'doc_id':
            if item[0] in  pmid2pcount:
                allpmids.append(item[0])
                PMID2CVD.append({'pmid':item[0],'cat':item[1]})
            
                if item[1] == '0':
                    dis0.append(item[0])
                elif item[1] == '1':     
                    dis1.append(item[0])
            
cvd2pmids.update({dis[0]:dis0,dis[1]:dis1})                   
                

with open('cat2pmids.json', 'w') as fp:
                json.dump(cvd2pmids, fp)
```
