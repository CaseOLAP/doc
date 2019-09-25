
# Download Pipeline

This pipeline downloads the data from the source server. It checks the integrity of the downloaded data and extracts it to the target directory. Following are the outline the download of the pipeline.

**1. Import Required Python packages**


```python
import os
import sys
import re
import time
import subprocess

```

**2. Specify source and target data directory**


```python
DATA_DIR = './'

'''FTP address for baseline directory of Pubmed '''
BASELINE_DIR = os.path.join(DATA_DIR,\
                            'ftp.ncbi.nlm.nih.gov/pubmed/baseline/')


'''FTP address for baseline directory of Pubmed'''
UPDATE_FILES_DIR = os.path.join(DATA_DIR,\
                                'ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/')
```

**3. Controlling download with ```os``` package and ```wget``` command.**

```os.path.join()``` functionality is implemented to join one or more path components intelligently. 
     ```os.system (wget command)```  functionality is implemented to execute the wget command in a subshell. 
     The following are optional syntax modifiers for the wget command to control the download:
 - flag  ```-q ``` turns off wget output,
 - flag ```-r ``` turns on recursive retrieving, 
 - directory prefix ``` --directory-prefix=%s```  sets the prefix of all other files and subdirectories,
 - directory based limit ```--no-parent```  guarantees that you will never leave the existing hierarchy of the directory.

  
  


```python
def download_pubmed_baseline():
    '''Baseline'''
    print("Start downloading pubmed baseline files.", "ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/")
    t1 = time.time()
    rc = os.system('wget -q -r --directory-prefix=%s --no-parent ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/' % DATA_DIR)
    if rc != 0:
        print("Return code of downloading pubmed baseline files via wget is %d, not zero." % rc)
        print("Link: ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline")
        exit(rc)
    t2 = time.time()
    print("Finish downloading pubmed baseline files. %fs" % (t2 - t1))
    
    
def download_pubmed_update():   
    '''Update'''
    print("Start downloading pubmed updatefiles.", "ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/")
    rc = os.system('wget -q -r --directory-prefix=%s --no-parent ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/' % DATA_DIR)
    if rc != 0:
        print("Return code of downloading pubmed update files via wget is %d, not zero." % rc)
        print("Link: ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles")
        exit(rc)
    t3 = time.time()
    print("Finish downloading pubmed updatefiles. %fs" % (t3 - t2))
    
    
def download_bioconcepts2pubtator_offsets():
    '''bioconcepts2pubtator'''
    rc = os.system('wget -q --directory-prefix=%s ftp://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator/bioconcepts2pubtator_offsets.gz' % DATA_DIR)
    if rc != 0:
        print("Return code of downloading pubmed update files via wget is %d, not zero." % rc)
        print("Link: ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles")
        exit(rc)
```

**4. Md5-checksum**

An MD5-checksum is a 32-character hexadecimal number that is computed on a file. This number helps to verify the integrity of the file download. Implementing the ```re``` and ```subprocess``` packages adds MD5-checksum functionality.



```python
def check_all_md5_in_dir(dir):
  
    if os.system("which md5sum 1>/dev/null") != 0:
        print("md5sum not found")           
        # Continue executing
        return
      
    count = 0
    print("==== Start checking md5 in %s ====" % dir)
    if os.path.isdir(dir):
        for file in os.listdir(dir):
            if re.search('^medline17n\d\d\d\d.xml.gz$', file):
                count += 1
                check_md5(os.path.join(dir, file))
                if count % 100 == 0:
                    print("%d files checked" % count)
        print("==== All md5 check succeeded (%d files) ====" % count) 
        
        
    else:
        print("Directory not found: %s (for md5 check)" % dir)
        
        
        

def check_md5(file):
    if os.path.isfile(file) and os.path.isfile(file + ".md5"):

        # Work only on Linux, user "md5" for Mac
        stdout = subprocess.check_output("md5sum %s" % file, shell=True).decode('utf-8')

        md5_calculated = re.search('[0-9a-f]{32}', stdout).group(0)
        md5 = re.search('[0-9a-f]{32}', open(file + ".md5", 'r').readline()).group(0)

        if md5 != md5_calculated:
            print("Error: md5 check failed for file %s" % file)
            exit(1)
```

**5. Data extraction** : 

Downloaded data files are in a compressed ```‘.gz’``` format, which need to be extracted. A data extraction pipeline can be created with the following steps:
- import regular expression ```(re)```, and ```subprocess``` modules,
- list all data files using ```os.listdir ``` functionality,
- with ```os``` and ```wget``` command, ```gunzip -fqk``` , extract all files in a loop.


```python
# Assume filename is *.gz
def extract(file):
    rc = os.system('gunzip -fqk %s' % file)
    if rc != 0:
        print("gunzip return code for file %s is %d, not zero" % (file, rc))
        exit(rc)
    return rc

def extract_all_gz_in_dir(dir):
    if os.path.isdir(dir):
        count = 0
        print("==== Start extracting in %s ====" % dir)
        t1 = time.time()
        for file in os.listdir(dir):
            if re.search('.*\.gz$', file):
                extract(os.path.join(dir, file))
                count += 1
                if count % 50 == 0:
                    print("%d files extracted, %fs taken so far" % (count, time.time() - t1))
        print("==== All files extracted (%d files). Total time: %fs ====" % (count, time.time() - t1))
    else:
        print("Directory not found: %s (for extraction)" % dir)        

```

6. **Run the pipeline**


```python
BASELINE_DIR = os.path.join(DATA_DIR, 'ftp.ncbi.nlm.nih.gov/pubmed/baseline/')
UPDATE_FILES_DIR = os.path.join(DATA_DIR, 'ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/')
        
            
'''Download'''    
download_pubmed_baseline()
download_pubmed_update()
    
'''MD5 Checksum'''
check_all_md5_in_dir(BASELINE_DIR)
check_all_md5_in_dir(UPDATE_FILES_DIR)
    
    
'''Extraction'''
extract_all_gz_in_dir(BASELINE_DIR)
extract_all_gz_in_dir(UPDATE_FILES_DIR)

'''bioconcepts2pubtator'''
download_bioconcepts2pubtator_offsets()
extract(os.path.join(DATA_DIR, 'bioconcepts2pubtator_offsets.gz'))

```


```python

```
