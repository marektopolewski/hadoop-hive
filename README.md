# Hadoop & Hive: Text Analytics


## Introduction
In order to index the Internet, Google parses vast amounts of web pages and count occurrences of different
words within the page, or execute other text analytics processes. To handle such huge volumes of information, 
systems must be designed in a **distributed** and **parallel** manner to ensure integrity and efficiency.


## MapReduce and Hadoop
This project explores the **MapReduce** padagrim which facilitates querying data in distributed systems. Its 
implementation over **Hadoop** allows the user to take advantage of the **HDFS** (Hadoop Distributed File 
System) which enables partitioning of files and distribution to **cluster nodes**. This allows us to move the 
inexpensive code to the data and not the opposite like in the classical quering frameworks.

MapReduce consists of three stages:
1. **Map** - Nodes apply the map function to the input data and temporarily store it as key-value pairs.
2. **Shuffle** - Data is redistributed, so that every node contains map outputs belonging to the same key. 
3. **Reduce** - The reduce function is applied to thew grouped data, e.g. max or sum.

![A simplified diagram illustrating the workings of a MapReduce program.](https://github.com/marektopolewski/hadoop-hive/blob/master/.assets/mapreduce.png)


## Hive
The "vanilla" Hadoop approach requires the programmer to implement the Mapper and Reducer, while the Shuffler
can be omitted for the majority of queries. Nonetheless, this technique can often be very tedious and prone 
to errors. For this purpose, a higher-level systems such as **Apache Hive**, that allow the user to build the 
desired query without writing lower-level Java code through the MapReduce paradigm and follows a SQL-like
syntax instead:
```
SELECT ss_store_sk, SUM(ss_net_profit) AS profit
FROM store_sales_1g
WHERE ss_store_sk IS NOT NULL and ss_net_profit IS NOT NULL and ss_sold_date_sk IS NOT NULL and
      ss_sold_date_sk >= 2451457 and ss_sold_date_sk <= 2451813
GROUP BY ss_store_sk
ORDER BY profit DESC
LIMIT 3;
```


## More
For further information read the `report.pdf`.