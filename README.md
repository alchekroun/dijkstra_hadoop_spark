# Dijkstra Project

## Description

Educational project given by our Big Data professor. The main goal is to implement the dijksktra algorithm using
the MapReduce paradigm and these two frameworks :
- Hadoop Streaming
- Spark

## Installation

For using this project you will need the following python libraries :
- Jupyter
- Spark
- Hadoop

## Usage

Our wish was to make an easily usable solution, so we decide to add a python library.

### Python library

To import it :

```python
import lib as dh
```

You can directly call the main function :
```python
dh.dijkstra(input_graph)
```

### Hadoop Streaming

#### Preprocess

You can preprocess the data you want to process with the following command :
```bash
cat data/<data>.dat | lib/preprocess.py >> data/<data_preprocessed>.dat
```

*An example of expected format input is given in* ````data/ex1.dat````

#### CLI way

You can use the ```mapper.py``` and the ```reducer.py``` with powershell as follows :
```bash
cat data/ex1_p.dat | lib/mapper.py | lib/reducer.py
```

You will need to do this multiple times, which is inconvenient. 

#### Hadoop streaming

Executing hadoop streaming job
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.8.1.jar \
                    -input data/<data>.dat -output output \
                    -file python/mapper.py -mapper mapper.py \
                    -file python/reducer.py -reducer reducer.py
```

### Spark

You can retrieve the spark implementation inside the jupyter notebook
```spark.ipynb```

## Authors

- [Estelle Hu](https://github.com/estellehu)

- [Alexandre Chekroun](https://github.com/alchekroun)

## References

- “Data-Intensive Text Processing with MapReduce” - Jimmy Lin and Chris Dyer
- Cormen, Thomas H.; Leiserson, Charles E.; Rivest, Ronald L.; Stein, Clifford (2001). "Section 24.3: Dijkstra's algorithm".
Introduction to Algorithms (Second ed.).
- [Wikipedia page](https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm)
- [Programiz page](https://www.programiz.com/dsa/dijkstra-algorithm)
- Hector Ortega-Arranz; Diego R. Llanos; Arturo Gonzalez-Escribano; Morgan & Claypool Publishers, Morgan and Claypool Life Sciences (2014). “The Shortest-Path Problem: Analysis and Comparison of Methods”
- [Bilal Elchami’s git repository](https://github.com/bilal-elchami/dijkstra-hadoop-spark)
