# python_kafka_read_messages_between_timestamps
Simple Python Kafka Code to read messages between two timestamps

## Motivation behind writing this code  
This is a basic problem, I want to find some messages that are missing in another system. In earlier time a consumer read messages but did not update the other system.   
  
However, I dont want to read all messages from the begining of time so I need code that can read messages between two timestamps and put the messages in a file. And I want to do some operation to these messages, so I dont want to use an external tool like "kcat".  
  
#### Note: This code assumes that the message is basic string, If you want to handle Deserialization or decompression you will have to add some additional code. You can additionally dump things like msg.key, msg.header if you like (at or after line 92 in code)  
  
   
   
## How to update configs required for the code?  
look at my.prop file for some configurations. You have to update these configurations:
```
[kafka]
KAFKA_IP=localhost:29092
TOPIC=sample
OUTFILE=kafka_messages.csv
```
  
Here 
KAFKA_IP = host:port for a kafka broker  
TOPIC = name of topic  
OUTFILE = where you want the message to be dumped  
  
On line number 118,119 inside kafkautil.py file
```
118     startts = os.environ.get('START_DATE', '2022-12-11T15:00:12')
119     endts = os.environ.get('END_DATE', '2022-12-11T16:10:12')
```
  
change the startts and enddts to your desired start and end time, 
#### Note: keep in mind that the Timezone will be based on Kafka machine's local OS setting. Like if the kafka server is runnin in UTC timezone, you will have to provide UTC time range.  
  
  
## How to execute the code?
* Please ensure that you are using Python 3.6 or higher
* Please install the packages mentioned in requirements.txt 
  ```
  pip install -r /path/to/requirements.txt
  or
  pip3 install -r /path/to/requirements.txt
  ```
* Once above two conditions are met, you can run:
  ```
  python kafkautil.py
  OR
  python3 kafkautil.py
  ```
This should output a message similar to the following, and also generate a file based on "{pwd}/output/OUTFILE" value:
```
Kafka Message Consuming between 2022-12-11T15:00:12 (1670751012000) : 2022-12-11T16:10:12 (1670755212000)
Topic:Partition count :: sample::1

Total 6 messages written to /tools/python/KafkaConsumeByTime/output/kafka_messages.csv
```

As you would be expecting by this point, this is part of a small tool that I am writing for some troubleshooting. I thought this kafka part is probably going to be useful for many people.
  
  
  
### References:  
------------------  
* https://www.programcreek.com/python/example/98439/kafka.TopicPartition  
* https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html?highlight=seek#
* https://github.com/dpkp/kafka-python/issues/648  
