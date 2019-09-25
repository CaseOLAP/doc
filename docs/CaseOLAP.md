
# CaseOLAP score calculation

**CaseOLAP score calculation** : CaseOLAP score are the quantification of user defined entity-category association. It start with the text-cube document structure and finds the entity in each document in each cell of the text-cube and by implementing updated text-cube metadata, it calculates the CaseOLAP score with following steps:

**Integrity **: Integrity of user defined phrase is taken to be 1. (Autophrase, Segphrase)

**Popularity** : It depends on how frequently a protein name is mentioned within one category, and it is calculated only using the statistics from the cells of documents pertaining to that individual category. Rare protein names in a cell are ranked low, while an increase in their frequency of mention has a diminishing return.
 - In each of the cell c in text-cube, term frequency ```tf(p,c)``` of each entity is calculated.
 -  Using individual term frequency for each entity(protein), total sum of the term frequency  cntP(c) is calculated.
 - A normalized popularity of phrase `p` in cell `c`, `pop(p,c)` is calculated by using `tf(p,c)` and `ntfP(c)` calculated in 6.2.1 and 6.2.2. [eq ref].

**Distinctiveness** : It is based on the relevance  of a entity name to a specific category by comparing the occurrence of the protein name in the target data set, i.e., the cell documents describing one cell, to the contrastive data set, i.e., the cells of documents describing the remaining cells.
 - Normalized term frequency ```ntf(p,c)``` [eq ref]  and normalized document frequency `ndf(p,c)` [eq ref] at 5.4.3 are used to  calculated the relevance score ```rel(p,c)``` [eq. ref] of protein `p` in cell `c`.
 - Normalized distinctiveness disti(p,c) [eq ref]  is calculated by using relevance score of protein p in cell c and and relevance across neighbouring cells `c’ (c’  K(p,c)` : neighbourhoods of cell `c`).

**CaseOLAP score**: It is the product of Integrity, Popularity and Distinctiveness calculated in 6.1,6.2 and 6.3.


##### Import required libraries


```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
%matplotlib inline
import json
```

#### Data


```python
with open('input/cat2pmids.json', 'r') as f:
    cvd2pmids = json.load(f)
print('total cat2pmids:',len(cat2pmids))
with open('input/pmid2pcount.json', 'r') as f:
    pmid2pcount = json.load(f)
print('total pmid2pcount:',len(pmid2pcount))
```

### Caseolap Score Calculation


```python
class Caseolap(object):
    
    
    
    
    def __init__(self,cvd2pmids,pmid2pcount):
        
        self.cellnames = []
        self.cvd2pmids = cvd2pmids
        self.pmid2pcount = pmid2pcount
        self.cell_pmids = {}
        self.cell_pmid2pcount = {}
        self.all_proteins = []
        self.cell_uniqp = {}
        self.cell_p2tf = {}
        self.cell_tf = {}
        self.cell_cntp = {}
        self.cell_pop = {}
        self.cell_p2pmid = {}
        self.cell_ntf ={}
        self.cell_ndf ={}
        self.cell_rel ={}
        self.cell_dist = {}
        self.cell_caseolap ={}
        
        
        
    def df_builder(self,cell_quant,fname): 
            flatdata = []
            for p in self.all_proteins:
                d = {'protein':p}
                for name in self.cellnames:
                    d.update({name:cell_quant[name][p]})
                flatdata.append(d)    
            df =  pd.DataFrame(flatdata)
            df = df.set_index('protein')
            df.to_csv('data/'+fname+'.csv')
            return df  
        
        
        
    def dump_json(self,data,fname):
            with open('data/'+fname+'.json', 'w') as dl:
                json.dump(data, dl)
            
        
        
    
    def cell_pmids_collector(self, dump = False,verbose =False):
            for key,value in self.cvd2pmids.items():
                cell_name = key
                cell_pmids = value
                self.cellnames.append(cell_name)
                self.cell_pmids.update({cell_name:cell_pmids})
                
                if verbose:
                    print('total pmids collected for cell - ',cell_name,len(cell_pmids))
            if dump:
                self.dump_json(self.cell_pmids,fname = 'cellpmids')
            
                
            
        
        
    def cell_pmid2pcount_collector(self):
            for key,value in self.cell_pmids.items():
                cell_name = key
                cell_pmids = value
                ipmid2pcount = {}
                for pmid in cell_pmids:
                    pmid_pcount = self.pmid2pcount[pmid]
                    ipmid2pcount.update({pmid:pmid_pcount})
                self.cell_pmid2pcount.update({cell_name:ipmid2pcount})
                
                
                
    def all_protein_finder(self,dump =False,verbose = False):
            allproteins = []
            for key,value in self.cell_pmid2pcount.items():
                cell_name = key
                cellpmid2pcount = value
                cellproteins = []
                for key, value in cellpmid2pcount.items():
                    pmid = key
                    pmid_pcount = value
                    for key,value in pmid_pcount.items():
                        allproteins.append(key)
                        cellproteins.append(key)
                    uprotein = list(set(cellproteins))
                self.cell_uniqp.update({cell_name:uprotein}) 
                
                if verbose:
                    print('total proteins collected for cell - ',cell_name,len(uprotein))
                    
            self.all_proteins = list(set(allproteins))
            
            if verbose:
                    print('total proteins collected: ',len(self.all_proteins))
            
            if dump:
                self.dump_json(self.all_proteins,fname = 'allproteins')
                self.dump_json(self.cell_uniqp,fname = 'unique_proteins')
                
                
                
                
    def cell_map(self,cellpmid2pcount,select):
                map_dict = []
                for key,value in cellpmid2pcount.items():
                    pmid = key
                    pmid_pcount = value
                    for key, value in pmid_pcount.items():
                        if select == 'tf':
                            map_dict.append({'protein': key, 'tf':int(value)})
                        elif select == 'pmid':
                            map_dict.append({'protein': key, 'pmid':pmid})
                return map_dict
        
                
                
                
    def cell_reduce(self,Dict,col,operation): 
                df = pd.DataFrame(Dict)
                df = df.set_index(col[0])
                
                if operation == 'sum':
                    gdf = df.groupby(col[0]).sum()
                elif operation == 'count':
                    gdf = df.groupby(col[0]).count()
                        
                index_name = list(gdf.index)
                csum = list(gdf[col[1]])
                ucount = {}
                for x,y in zip(index_name,csum):
                    ucount.update({x:y})
                return ucount
        
                
            
    
    def cell_p2tf_finder(self):      
            for key,value in self.cell_pmid2pcount.items():
                cell_name = key
                cellpmid2pcount = value
                
                '''map-reduce'''
                CellP2tf = self.cell_map(cellpmid2pcount,select ='tf')
                cellp2tf = self.cell_reduce(CellP2tf,['protein','tf'],operation = 'sum')  
                
                self.cell_p2tf.update({cell_name:cellp2tf})
                
                
                
                
    def cell_tf_finder(self):       
                for key, value in self.cell_p2tf.items():
                    cell_name = key
                    cellp2tf = value
                    celltf = {}
                    for p in self.all_proteins:
                        if p in self.cell_uniqp[cell_name]:
                            celltf.update({p:cellp2tf[p]})
                        else:
                            celltf.update({p:0})
                            
                    self.cell_tf.update({cell_name:celltf})
                
                
                
                   
    def cell_pop_finder(self,dump=False):
            for key,value in self.cell_tf.items():
                cell_name = key
                cell_tf = value
                cellpop = {}
                cntp = 0
                #----------------------------
                for key,value in cell_tf.items():
                        cntp = cntp+int(value)
                self.cell_cntp.update({cell_name:cntp})
                #------------------------------
                for key,value in cell_tf.items():
                        pop = np.log(value +1)/np.log(cntp)
                        cellpop.update({key:pop})
                self.cell_pop.update({cell_name:cellpop})
                
            if dump:
                self.df_builder(self.cell_pop,fname = 'pop')
                
                
                
                
    def cell_p2pmid_finder(self):
            for key,value in self.cell_pmid2pcount.items():
                cell_name = key
                cellpmid2pcount = value
                
                '''map-reduce'''
                CellP2pmid = self.cell_map(cellpmid2pcount,select = 'pmid')
                cellp2pmid = self.cell_reduce(CellP2pmid,['protein','pmid'],operation = 'count') 
                      
                self.cell_p2pmid.update({cell_name:cellp2pmid})   
                
                
                
                
    def cell_ntf_finder(self):
            k1 = 1.2
            b = 0.75
            for key,value in self.cell_tf.items():
                cell_name = key
                celltf = value
                #----------------------------
                nonzero_celltf = []
                for key,value  in celltf.items():
                    if int(value)>0:
                        nonzero_celltf.append(int(value))
                #-------------------------------------------        
                av_cntp = self.cell_cntp[cell_name]/float(len(nonzero_celltf))
                cellntf = {}
                
                for key,value in celltf.items():
                    p = key
                    tf = value
                    ntf = (tf*(k1+1))/float(tf+(k1*(1-b+(b*(self.cell_cntp[cell_name]/float(av_cntp))))))
                    cellntf.update({p:ntf})
                    
                self.cell_ntf.update({cell_name:cellntf})
                
                
                
                
    def cell_ndf_finder(self):
            for key,value in self.cell_p2pmid.items():
                cell_name = key
                cellp2pmid = value
                all_pmid_counts = []
                cellndf = {}
                
                #--------------------------------------------
                for key,value in cellp2pmid.items():
                    all_pmid_counts.append(value)
                maxv = max(all_pmid_counts) 
                #-----------------------------------------
                
                for p in self.all_proteins:
                    if p in self.cell_uniqp[cell_name]:
                        c = cellp2pmid[p]
                        ndf = np.log(1 + c)/np.log(1 + maxv)
                    else:
                        ndf = 0
                    cellndf.update({p:ndf}) 
                    
                self.cell_ndf.update({cell_name:cellndf})
                
                
                
                
    def cell_rel_finder(self):
            for key,value in self.cell_ntf.items():
                cell_name = key
                cellntf = value
                cellrel = {}
                for p in self.all_proteins:
                    rel = cellntf[p]*self.cell_ndf[cell_name][p]
                    cellrel.update({p:rel})
                self.cell_rel.update({cell_name:cellrel})
                
                
                
            
    def cell_dist_finder(self,dump=False): 
            cell_exprel = {}
            for key,value in self.cell_rel.items():
                cell_name = key
                cellrel = value
                cellexprel = {}
                for key,value in cellrel.items():
                    cellexprel.update({key:np.exp(value)})
        
                cell_exprel.update({cell_name:cellexprel})
            
            #-----------------------------------------------------
            p2din = {}
            for p in self.all_proteins:
                din = 1.0
                for cellname in self.cellnames:
                    din = din + cell_exprel[cellname][p]
                p2din.update({p:din})
                
            #--------------------------------------------------------
        
            for key,value in cell_exprel.items():
                cell_name = key
                cellexprel = value
                celldist = {}
                for key,value in cellexprel.items():
                    celldist.update({key:value/p2din[key]})
        
                self.cell_dist.update({cell_name:celldist})
            
            if dump:
                self.df_builder(self.cell_dist,fname = 'dist')
            
            
            
            
    def cell_cseolap_finder(self,dump=False):
            for key,value in self.cell_dist.items():
                cell_name = key
                celldist = value
                cellcaseolap = {}
                for key,value in celldist.items():
                    cellcaseolap.update({key:(value*self.cell_pop[cell_name][key])})
                self.cell_caseolap.update({cell_name:cellcaseolap})
                
            if dump:
                self.df_builder(self.cell_caseolap,fname = 'caseolap')
                self.dump_json(self.cell_caseolap,fname = 'caseolap')
    
                      
```

### Test Run


```python
C = Caseolap(cvd2pmids,pmid2pcount)
C.cell_pmids_collector(dump =True,verbose =True)
#C.cell_pmids
C.cell_pmid2pcount_collector()
#C.cell_pmid2pcount
C.all_protein_finder(dump =True,verbose = True)
#C.all_proteins
C.all_protein_finder()
#C.all_proteins
#C.cell_uniqp   
C.cell_p2tf_finder()
#C.cell_p2tf
C.cell_tf_finder()
#C.cell_tf
C.cell_pop_finder(dump=True)
#C.cell_pop
C.cell_p2pmid_finder()
#C.cell_p2pmid
C.cell_ntf_finder()
#C.cell_ntf
C.cell_ndf_finder()
#C.cell_ndf
C.cell_rel_finder()
#C.cell_rel
C.cell_dist_finder(dump=True)
#C.cell_dist
C.cell_cseolap_finder(dump=True)
#C.cell_caseolap
```
