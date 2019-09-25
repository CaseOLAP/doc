
# Text-cube creation 



Text-cube creation is an intelligent data engineering step in CaseOLAP which outputs a functional document structure with dimensions and cells informed by user provided document metadata. Each cell within the text-cube corresponds to a subset of documents. Following are the steps to create a text-cube.

**Selection of user defined categories** :  User selects the MeSH descriptors associated with the defined categories. Using those MeSH descriptors, cells of the documents are prepared. MeSH to PMID mapping prepared at step 2.4 is used to populate user defined cells in text-cube.

**Implementation of MeSH descriptors** :  U.S. National Library of Medicine provides the Medical Subject Heading (MeSH) in the hierarchical tree(data structure) with node ids. This permits searching for publications at varying levels of specificity. With given set of root node ids, one can select all documents for a specific cell by collecting all descendant nodes. 

**Text-cube metadata preparation** : A collection of user provided metadata(cell name, associated MeSH, PMID etc) representing each text document in the cell is prepared. There could be the significant number of  documents falling under two or more cells. A dimensional hierarchy is implemented to organize the text-cube, providing each cell with a specific cell context (e.g., a parent cell, child cell, or sibling cell). Following are the steps to prepare cell-document metadata preparation:
- provide the name of the cell,
- make a list of document id (PMID) within each cell,
- count the number of documents in each cell.



#### Import required libraries


```python
import json
import sys
import time

```

**Set up input and out file address**


```python
input_file_meshtree = "input/mtrees2018.bin"  # MeSH Tree 
input_file_mesh2pmid = "mesh2pmid.json" # MeSH to PMID mapping
input_file_input_cat = 'input/categories.txt'# file containing MeSH root nodes for each category  
output_file_pmid_and_cat = "pmid_and_cat.json" # file containing pmid to cell mapping

concerned_cat = []
```

#### Collection of all decendent MeSH nods in MeSH tree


```python
"""
Find corresponding MeSH Terms for each category.
"""
with open(input_file_input_cat, "r") as f_in_input_cat:
    for line in f_in_input_cat:
        concerned_cat.append(line.strip().split())
num_cat = len(concerned_cat)

term_set_per_cat = [set() for _ in range(num_cat)]
with open(input_file_meshtree, "r") as f_in_meshtree:
    for line in f_in_meshtree:
        term_tree = line.strip().split(";")
        cur_term = term_tree[0]
        cur_tree = term_tree[1]

        for i in range(num_cat):
            for cur_cat_tree in concerned_cat[i]:
                if cur_cat_tree in cur_tree:
                    term_set_per_cat[i].add(cur_term)
                          
```

#### Application of ```Mesh to PMID mapping``` to find documents for each cell


```python
"""
Find corresponding papers for each category.
"""

pmid_set_per_cat = [set() for _ in range(num_cat)]
with open(input_file_mesh2pmid, "r") as f_in:
    
    start = time.time()
        
    k = 0
    for line in f_in: 
        
        mesh2pmid = {}
        Info = json.loads(line.strip())
        for key,value in Info.items():
            mesh2pmid.update({key:value})
        
        k = k+1
        if k%1000 ==0:
            print(k,'done!')
            #break

        for i in range(num_cat):
            for cur_term in term_set_per_cat[i]:
                if cur_term == key:
                    pmid_set_per_cat[i] = pmid_set_per_cat[i] | set(mesh2pmid[cur_term])      
```

#### Creation of PMID to Cell mapping


```python
"""
Serialize papers
"""
pmid_and_cat = []
for i in range(num_cat):
    for cur_pmid in pmid_set_per_cat[i]:
        pmid_and_cat.append([cur_pmid, i])

with open(output_file_pmid_and_cat, "w") as f_out:
    json.dump(pmid_and_cat, f_out)
```
