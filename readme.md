## Hadoop-- Page Rank with dead-end data

![alt tag](equation.png)
With dead-ends data, it needs to be normalized.  

rjnew = normalization of rj  
rjnew' =  rj in next iteration   

In this project, I designed a 5-job architecture.    
1. Count Job: count the number of nodes.
2. Map Job: Create link map.
3. PageRank Job: calculate page rank rjnew.
4. Sum Job: Calculate the sum of PageRank  S.
5. Sort Job: Do the final normalization and sort the output.

It will iterate between PageRank Job and Sum Job until the iteration times you set.  

### Map Job
Input: line
> 1   2  
> 1   3  
> 2   3  
Output:Node (pageNode|PageRank|outlink)  
> 1|1/N|2,3  
> 2|1/N|3  

### PageRank Job
#### Mapper
Input: line
> 1|1/N|2,3  
> 2|1/N|3  

Create two kind of node, OUT node for recording the link map, IN node for calculate PageRank.  
Output: <pageNode , Node>  

> 1		1|r1|2,3|OUT  
> 2		1|r2new|IN  
> 3		1|r3new|IN  
> 2		2|r2|3|OUT  
> 3 	2|r3new|IN  

#### Reducer
Sum page rank of Nodes which is IN type, and repalce the PageRank.
Output: <Node>
> 1		1|r1new'|2,3  
> 2		2|r1new'|3  
> 3 	2|r3new'  


