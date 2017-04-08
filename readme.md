## Hadoop-- Page Rank with dead-end data

This project implements page rank algorithm.  
$$ r_j = \sum{i→j} \beta\frac{r_i}{d_i} +(1-\beta)\frac{1}{N} $$
With dead-ends data, it needs to be normalized.
$$ {r_j}^{new} = r_j + \frac{1-S}{N} $$
$$ S = \sum{j} r_j $$

$$ r_j = \sum{i→j} \beta\frac{r_i}{d_i} +(1-\beta)\frac{1}{N} $$
$${r_j}^{new}$$ = normalization of $${r_j}$$
$${r_j}^{new'}$$ =  $${r_j}$$ in next iteration 

In this project, I designed a 5-job architecture.  
1. Count Job: count the number of nodes.
2. Map Job: Create link map.
3. PageRank Job: calculate page rank $${r_j}^{new}$$.
4. Sum Job: Calculate the sum of page rank  S.
5. Sort Job: Do the final normalization and sort the output.

It will iterate between PageRank Job and Sum Job until the iteration times you set.  

###Map Job
Input: line
> 1   2
> 1   3
> 2   3
Output:Node (pageNode|pageRank|outlink)
> 1|$$\frac{1}{N}$$|2,3
> 2|$$\frac{1}{N}$$|3

###PageRank Job
####Mapper
Input: line
> 1|$$\frac{1}{N}$$|2,3
> 2|$$\frac{1}{N}$$|3
Create two kind of node, OUT node for recording the link map, In node for calculate page rank. 
Output: <pageNode , Node>
> 1		1|$${r_1}$$|2,3|OUT
> 2		1|$${r_2}^{new}$$|IN
> 3		1|$${r_3}^{new}$$|IN
> 2		2|$${r_2}$$|3|OUT
> 3 	2|$${r_3}^{new}$$|IN

####Reducer
Sum page rank of Nodes which is IN type, and repalce the page rank.
Output: <Node>
> 1		1|$${r_1}^{new'}$$|2,3
> 2		2|$${r_1}^{new'}$$|3
> 3 	2|$${r_3}^{new'}$$


