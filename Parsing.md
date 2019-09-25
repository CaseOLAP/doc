
# Data Parsingn Pipeline

This pipeline will parse the extracted data and convert it into data structures compatible with the CaseOLAP pipeline.

**Installation of required python package** : Install and import ```lxml, itertools``` and ```json```  libraries into the current python environment.


```python
import re
import itertools
import json
import sys
import os
import time
import traceback
from lxml import etree
```

###  Set up output data dir


```python
DATA_DIR = './'
```

MeSH statistics dictionary


```python
mesh_statistics = {}
```

**Data parsing strategy ** :  The extracted data is an ```XML``` file, and text data is embedded in the tree structure of ```XML``` document. The following are steps for parsing data:
- Implement  the etree functionality  in ```lxml``` module to dig into the tree structure of ```XML``` document. 
- The separate components e.g. ```PMID, authors, abstract```, ```MeSH``` etc. of the data is obtained by using tags representing these components. 
- Implement the chain functionality in itertools  to creates an iterator that returns elements from the iterables which was  obtained by implementing  etree functionality in ```lxml```.



```python
# Search the tag in the xml element
# Return tag's text if tag exists, return empty string if doesn't
def get_text(element, tag):
    e = element.find(tag)
    if e is not None:
        return e.text
    else:
        return ''

# <!ELEMENT	AuthorList (Author+) >
# <!ELEMENT	Author (((LastName, ForeName?, Initials?, Suffix?)\
#                | CollectiveName), Identifier*, AffiliationInfo*) >
def parse_author(authors):
    result = []
    for author in authors:
        item = {}
        item['LastName'] = get_text(author, 'LastName')
        item['ForeName'] = get_text(author, 'ForeName')
        item['Initials'] = get_text(author, 'Initials')
        item['Suffix'] = get_text(author, 'Suffix')
        item['CollectiveName'] = get_text(author, 'CollectiveName')
        result.append(item)
    return result
```

**Creation of dictionary of parsed data **: A python dictionary  is created with all the components as key-value pair. This JSON-like data structure makes it compatible for  indexing and searching  in Elasticsearch which is described in step 3 of protocol.

**Creation of MeSH to PMID mapping**: During the creation of dictionary of parsed data, MeSH to PMID mapping table can also be created. This mapping is used to create Text-cube(in step 4) document structure as an requirement of CaseOLAP algorithm.


```python
def parse_pubmed_file(file, pubmed_output_file, pmid2mesh_output_file):

    print('Start parsing %s' % file)
    sys.stdout.flush()

    t1 = time.time()

    f = open(file, 'r')

    tree = etree.parse(f)
    articles = itertools.chain(tree.findall('PubmedArticle'), tree.findall('BookDocument'))
    count = 0

    noabs = 0
    
    for article in articles:

        count += 1
        result = {}
        pmid2mesh = {}

        # PMID - Exactly One Occurrance
        result['PMID'] = get_text(article, './/PMID')
        pmid2mesh['PMID'] = get_text(article, './/PMID')

        # # Article title - Zero or One Occurrences
        # result['ArticleTitle'] = get_text(article, './/ArticleTitle')

        # Abstract - Zero or One Occurrences
        abstractList = article.find('.//Abstract')
        if abstractList != None:
            try:
                abstract = '\n'.join([line.text for line in abstractList.\
                                      findall('AbstractText')])
                result['Abstract'] = abstract
            except:
                result['Abstract'] = ''
                noabs += 1
        else:
            result['Abstract'] = ''
            noabs += 1
            
            

            
            
        # # Author List - Zero or More Occurrences
        # authors = article.findall('.//Author')
        # result['AuthorList'] = parse_author(authors)
        
        # # Journal - Exactly One Occurrance
        # journal = article.find('.//Journal')
        # result['Journal'] = get_text(journal, 'Title')
        
        
        result['PubDate'] = {}
        result['PubDate']['Year'] = get_text(journal, 'JournalIssue/PubDate/Year')
        
        # result['PubDate']['Month'] = get_text(journal, 'JournalIssue/PubDate/Month')
        # result['PubDate']['Day'] = get_text(journal, 'JournalIssue/PubDate/Day')
        # result['PubDate']['Season'] = get_text(journal, 'JournalIssue/PubDate/Season')
        # result['PubDate']['MedlineDate'] = get_text(journal,\
        #                                   'JournalIssue/PubDate/MedlineDate')
        
        
        
        
        

        # MeshHeading - Zero or More Occurrences
        headings = article.findall('.//MeshHeading')
        
        result['MeshHeadingList'] = []
        pmid2mesh['MeshHeadingList'] = []
        if headings:
            for heading in headings:
                descriptor_names = heading.findall('DescriptorName')
                qualifier_names = heading.findall('QualifierName')
                if descriptor_names:
                    for descriptor_name in descriptor_names:
                        result['MeshHeadingList'].append(descriptor_name.text)
                        pmid2mesh['MeshHeadingList'].append(descriptor_name.text)
                if qualifier_names:
                    for qualifier_name in qualifier_names:
                        result['MeshHeadingList'].append(qualifier_name.text)
                        pmid2mesh['MeshHeadingList'].append(qualifier_name.text)
                        
                        
        
        
        mesh_count = len(result['MeshHeadingList'])
        
        if mesh_count in mesh_statistics:
            mesh_statistics[mesh_count] += 1
        else:
            mesh_statistics[mesh_count] = 1

        # Dump to pubmed json file <----------------------------
        json.dump(result, pubmed_output_file)
        pubmed_output_file.write('\n')
        
        # Dump to pmid2mesh json file <-------------------------
        json.dump(pmid2mesh, pmid2mesh_output_file)
        pmid2mesh_output_file.write('\n')


    print('Finish parsing %s, totally %d articles parsed. Total time: %fs'\
                             % (file, count, time.time() - t1))
    print('%d acticles no abstracts' % (noabs))
    sys.stdout.flush()
    f.close()
```

**Setting up directories in parsing loop**


```python
def parse_dir(source_dir, pubmed_output_file,pmid2mesh_output_file):
    if os.path.isdir(source_dir):
        for file in os.listdir(source_dir):
            if re.search(r'^pubmed18n\d\d\d\d.xml$', file) is not None:
                try:
                    parse_pubmed_file(os.path.join(source_dir, file),\
                                      pubmed_output_file, pmid2mesh_output_file)
                except:
                    print("XXXX Unexpected error happended when parsing %s XXXX" % file)
                    print(traceback.print_exc())
                    sys.stdout.flush()
                    
                    
                    
                    


```

**Run the Parsing Pipeline**


```python
t1 = time.time()

pubmed_output_file_path = os.path.join(DATA_DIR, 'data/pubmed.json')
pmid2mesh_output_file_path = os.path.join(DATA_DIR, 'pmid2mesh/pmid2mesh_from_parsing.json')
    
    
    
pubmed_output_file = open(pubmed_output_file_path, 'w')
pmid2mesh_output_file = open(pmid2mesh_output_file_path, 'w')
    
    
    
parse_dir(os.path.join(DATA_DIR, 'ftp.ncbi.nlm.nih.gov/pubmed/baseline'),\
              pubmed_output_file, pmid2mesh_output_file)
parse_dir(os.path.join(DATA_DIR, 'ftp.ncbi.nlm.nih.gov/pubmed/updatefiles'),\
              pubmed_output_file, pmid2mesh_output_file)
    
    
    
pubmed_output_file.close()
pmid2mesh_output_file.close()

mesh_file = open(os.path.join(DATA_DIR, 'data/mesh_statistics.json'), 'w')
json.dump(mesh_statistics, mesh_file)
mesh_file.close()

print("==== Parsing finished, results dumped to %s ====" % pubmed_output_file_path)
print("==== TOTAL TIME: %fs ====" % (time.time() - t1))
```

### MeSH to PMID Mapping


```python
inputFilePath = "data/pubmed.json"
meshFilePath = "mesh2pmid/"
    
mesh2pmid_output_file = open(meshFilePath + 'mesh2pmid.json', "w") 
mesh2pmid = dict()
    
    with open(inputFilePath, "r") as fin:
        
        start = time.time()
        k = 0
            
        for line in fin: ## each line is single document
            
            try:
                k = k+1
                paperInfo = json.loads(line.strip())
                
                data_dict = {}
                
                # update PMID
                data_dict["pmid"] = paperInfo.get("PMID", "-1")
                
                #update MeSH Heading <----------------------
                data_dict["mesh_heading"] = " ".join(paperInfo["MeshHeadingList"])
                
                # collect Mesh2PMID <-------------------
                if data_dict["pmid"] != "-1":
                    for mesh in paperInfo["MeshHeadingList"]:
                        if mesh not in mesh2pmid:
                               mesh2pmid[mesh] = []
                        mesh2pmid[mesh].append(data_dict["pmid"])
                        
                        
                          
                if k%500000 == 0:
                    print(k,'done!')
                    #break
                    
            except:
                print("XXXX Unexpected Error happened at line: XXXX")
               
                
          
        
        
        
        # Dumping rest papers
        for key,value in mesh2pmid.items():
            json.dump({key:value}, mesh2pmid_output_file)
            mesh2pmid_output_file.write('\n')
            
        mesh2pmid = dict()
        
        
        

        end = time.time()
        print("Finish Total escaped time %s (seconds) " % (end - start) )
        
```
