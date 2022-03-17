# MapReduce TP 1

-> implementation of MapReduce framework


## input 

the file `ventes.txt`, contains some sort of laptops sales list, in this form :

| year | city | product_vendor | price | 
| ---- | ---- | -------------- | ----- |

## Output of the Job : `Application1`

purpose of this job is to calculate total of sales per city, the output will take the fomat :

| city | sales_total |
| ---- | ----------- |

take a look on the output in the file `/output1/part-00000` 


## Output of the Job : `Application2`


calculate total of sales Grouped by year & city, the output will take the fomat:

| city_year | sales_total |
| ---- | ----------- |

in my solution i used a composite key between mapper & reducer, this key type is defined in `job2/MyCustomKey.java`.
there is another idea, is to use concatenation of year & city as one single key, instead of a custom key.
take a look on the output in the file `/output2/part-00000`

