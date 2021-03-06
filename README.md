# Public Transit Status with Apache Kafka

Using public data from the [Chicago Transit Authority](https://www.transitchicago.com/data/), we will construct a streaming event pipeline around Apache Kafka, and its ecosystem, that allows us to simulate and display the status of train lines in real time on a Python web app.

## Description

Using Kafka and ecosystem tools like REST Proxy and Kafka Connect, we will develop a dashboard to display system status for the commuters of the Chicago Transit Authority (CTA). 

The architecture is given below:

![Project Architecture](images/diagram.png)

## Directory Layout
The project consists of two main directories, `producers` and `consumers`.

```

├── consumers
│   ├── consumer.py                         
│   ├── faust_stream.py 				    
│   ├── ksql.py						        
│   ├── models
│   │   ├── lines.py
│   │   ├── line.py                     
│   │   ├── station.py                  
│   │   └── weather.py                  
│   ├── requirements.txt
│   ├── server.py
│   ├── topic_check.py
│   └── templates
│       └── status.html
└── producers
    ├── connector.py 					    | Kafka JDBC Source Connector
    ├── models
    │   ├── line.py
    │   ├── producer.py					    | Python Client library
    │   ├── schemas
    │   │   ├── arrival_key.json
    │   │   ├── arrival_value.json
    │   │   ├── turnstile_key.json
    │   │   ├── turnstile_value.json
    │   │   ├── weather_key.json
    │   │   └── weather_value.json
    │   ├── station.py 					    | Python Client library
    │   ├── train.py
    │   ├── turnstile.py 				    | Python Client library
    │   ├── turnstile_hardware.py
    │   └── weather.py 					    | REST Proxy
    ├── requirements.txt
    └── simulation.py
    
```


## Components of the web app

### Kafka Producers for simulated train data
The first step in our plan is to configure the train stations to emit some of the events that we need. The CTA has placed a sensor on each side of every train station that can be programmed to take an action whenever a train arrives at the station. 

### Kafka REST Proxy Producer for simulated weather data
Our partners at the CTA have asked that we also send weather readings into Kafka from their weather hardware. Unfortunately, this hardware is old and we cannot use the Python Client Library due to hardware restrictions. Instead, we are going to use HTTP REST to send the data to Kafka from the hardware using Kafka's REST Proxy.

### Fetch station data on PostgreSQL via Kafka Connect
Finally, we need to extract station information from our PostgreSQL database into Kafka. We've decided to use the [Kafka JDBC Source Connector](https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/index.html).

* You can run this file directly to test your connector, rather than running the entire simulation.
* Make sure to use the [Landoop Kafka Connect UI](http://localhost:8084) and [Landoop Kafka Topics UI](http://localhost:8085) to check the status and output of the Connector.
* To delete a misconfigured connector: `CURL -X DELETE localhost:8083/connectors/stations`


### Faust Stream Processor to transform station data
We will leverage Faust Stream Processing to transform the raw Stations table that we ingested from Kafka Connect. The raw format from the database has more data than we need, and the line color information is not conveniently configured. To remediate this, we're going to ingest data from our Kafka Connect topic, and transform the data.

You must run this Faust processing application with the following command:

`faust -A faust_stream worker -l info`

### Aggregate turnstile data with KSQL Table
Next, we will use KSQL to aggregate turnstile data for each of our stations. Recall that when we produced turnstile data, we simply emitted an event, not a count. What would make this data more useful would be to summarize it by station so that downstream applications always have an up-to-date count

#### Tips

* You can run this file on its own simply by running `python ksql.py`
* Made a mistake in table creation? `DROP TABLE <your_table>`. If the CLI asks you to terminate a running query, you can `TERMINATE <query_name>`


### Kafka Consumers for the web app
With all of the data in Kafka, our final task is to consume the data in the web server that is going to serve the transit status pages to our commuters.


## Running and Testing

To run the simulation, you must first start up the Kafka ecosystem on the machine using Docker Compose.

```%> docker-compose up```

Docker compose will take a 3-5 minutes to start, depending on your hardware. Please be patient and wait for the docker-compose logs to slow down or stop before beginning the simulation.

Once docker-compose is ready, the following services will be available:

| Service | Host URL | Docker URL | Username | Password |
| --- | --- | --- | --- | --- |
| Public Transit Status | [http://localhost:8888](http://localhost:8888) | n/a | ||
| Landoop Kafka Connect UI | [http://localhost:8084](http://localhost:8084) | http://connect-ui:8084 |
| Landoop Kafka Topics UI | [http://localhost:8085](http://localhost:8085) | http://topics-ui:8085 |
| Landoop Schema Registry UI | [http://localhost:8086](http://localhost:8086) | http://schema-registry-ui:8086 |
| Kafka | PLAINTEXT://localhost:9092, PLAINTEXT://localhost:9093, PLAINTEXT://localhost:9094 | PLAINTEXT://kafka0:9092, PLAINTEXT://kafka1:9093, PLAINTEXT://kafka2:9094 |
| REST Proxy | [http://localhost:8082](http://localhost:8082/) | http://rest-proxy:8082/ |
| Schema Registry | [http://localhost:8081](http://localhost:8081/ ) | http://schema-registry:8081/ |
| Kafka Connect | [http://localhost:8083](http://localhost:8083) | http://kafka-connect:8083 |
| KSQL | [http://localhost:8088](http://localhost:8088) | http://ksql:8088 |
| PostgreSQL | `jdbc:postgresql://localhost:5432/cta` | `jdbc:postgresql://postgres:5432/cta` | `cta_admin` | `chicago` |

Note that to access these services from your own machine, you will always use the `Host URL` column.

When configuring services that run within Docker Compose, like Kafka Connect you must use the **Docker URL**. When you configure the JDBC Source Kafka Connector, for example, you will want to use the value from the `Docker URL` column.

### Running the Simulation

There are two pieces to the simulation, the `producer` and `consumer`. However, to verify the end-to-end system, it is critical that you open a terminal window for each piece and run them at the same time. 

#### To run the `producer`:

1. `cd producers`
2. `virtualenv venv`
3. `. venv/bin/activate`
4. `pip install -r requirements.txt`
5. `python simulation.py`

Once the simulation is running, you may hit `Ctrl+C` at any time to exit.

#### To run the Faust Stream Processing Application:
1. `cd consumers`
2. `virtualenv venv`
3. `. venv/bin/activate`
4. `pip install -r requirements.txt`
5. `faust -A faust_stream worker -l info`


#### To run the KSQL Creation Script:
1. `cd consumers`
2. `virtualenv venv`
3. `. venv/bin/activate`
4. `pip install -r requirements.txt`
5. `python ksql.py`

#### To run the `consumer`:

** NOTE **: Do not run the consumer until all the above steps are complete!
1. `cd consumers`
2. `virtualenv venv`
3. `. venv/bin/activate`
4. `pip install -r requirements.txt`
5. `python server.py`

Once the server is running, you may hit `Ctrl+C` at any time to exit.

We will be able to monitor a website to watch trains move from station to station on an interface ![like this](images/ui.png)

## Prerequisites

The following are required to run this web app :

* Docker
* Python 3.7
* A computer with a minimum of 16gb+ RAM and a 4-core CPU to execute the simulation

## Documentation
The following examples and documentation have been helpful in creating this web app :

* [Confluent Python Client Documentation](https://docs.confluent.io/current/clients/confluent-kafka-python/#)
* [Confluent Python Client Usage and Examples](https://github.com/confluentinc/confluent-kafka-python#usage)
* [REST Proxy API Reference](https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name))
* [Kafka Connect JDBC Source Connector Configuration Options](https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html)

