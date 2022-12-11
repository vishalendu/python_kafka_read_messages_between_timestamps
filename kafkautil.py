from configparser import ConfigParser
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from datetime import datetime
import os,json
from pathlib import Path
import pandas as pd

class kafkautil:
    
    configs=None
    KAFKA_IP=None
    TOPIC=None
    PARTITIONS=None
    defconsumer=None
    sqlitedbfile=None
    OUTDIR=None
    OUTFILE=None


    def __init__(self,propfile,OUTDIR):
        self.OUTDIR=OUTDIR
        self.configs = ConfigParser()
        self.configs.read(propfile)
        self.KAFKA_IP=self.configs.get('kafka','KAFKA_IP')
        self.TOPIC=self.configs.get('kafka','TOPIC')
        self.OUTFILE=self.configs.get('kafka','OUTFILE')
        self.defconsumer=self.initconsumer("troubleshoot")


    def initconsumer(self,group):
        kconsumer = KafkaConsumer(bootstrap_servers=self.KAFKA_IP,
                                        auto_offset_reset='earliest',
                                        consumer_timeout_ms=1000,
                                        group_id=group)
        self.PARTITIONS= len(kconsumer.partitions_for_topic(self.TOPIC))
        return kconsumer
    
    # generate dictionary with {TopicPartition: Offsets}
    def offsets_for_times(self,partitions, timestamp):
        """Augment KafkaConsumer.offsets_for_times to not return None
        Returns
        -------
        dict from kafka.TopicPartition to integer offset
        """
        # Kafka uses millisecond timestamps
        response = self.defconsumer.offsets_for_times({p: timestamp for p in partitions})
        offsets = {}
        for tp, offset_and_timestamp in response.items():
            if offset_and_timestamp is None:
                # No messages exist after timestamp. Fetch latest offset.
                self.defconsumer.assign([tp])
                self.defconsumer.seek_to_end(tp)
                offsets[tp] = self.defconsumer.position(tp)
            else:
                offsets[tp] = offset_and_timestamp.offset
        return offsets
    
        ## datevar should be in this format: "2022-12-06T06:15:12"
    def getepoch(self,datevar):
        return datetime.strptime(datevar, "%Y-%m-%dT%H:%M:%S").strftime('%s')

    # Extract partition and offsets from the dictionary of {TopicPartition:offset}
    def getoffsetlist(self,offsets):
        mydict={}
        for k,v in offsets.items():
            mydict[k.partition]=v
        return mydict

    ## entry point to get start/end offset for all partitions
    def getOffsets(self,start,end):
        print(f"Topic:Partition count :: {self.TOPIC}::{self.PARTITIONS}")
        start_offsets=self.offsets_for_times([TopicPartition(self.TOPIC,i) for i in range(self.PARTITIONS)],start)
        end_offsets=self.offsets_for_times([TopicPartition(self.TOPIC,i) for i in range(self.PARTITIONS)],end)
        return [self.getoffsetlist(start_offsets),self.getoffsetlist(end_offsets)]
    

    ## This method writes to SQLite database and to csv file
    def consumeMsg(self,KAFKA_IP,TOPIC,start_offsets,end_offsets):
        messagelist=[]
        consumer2 = KafkaConsumer(group_id='troubleshooting-consumer',
                            consumer_timeout_ms=1000,
                            bootstrap_servers=self.KAFKA_IP)
        for i in range(len(start_offsets)):
            partition = TopicPartition(self.TOPIC, i)
            start = {start_offsets[i]}.pop()
            end = {end_offsets[i]}.pop()
            consumer2.assign([partition])
            consumer2.seek(partition, start)
            for msg in consumer2:
                if msg.offset < end:
                    messagelist.append(msg.value.decode('utf-8'))
                else:
                    break
        consumer2.close()
        return messagelist
    
    
    def doWork(self,START_DATE,END_DATE):
        start = int(self.getepoch(START_DATE))*1000
        end = int(self.getepoch(END_DATE))*1000
        print(f"Kafka Message Consuming between {START_DATE} ({start}) : {END_DATE} ({end})")
        start_offsets,end_offsets=self.getOffsets(start,end)
        df = pd.DataFrame(self.consumeMsg(self.KAFKA_IP,self.TOPIC,start_offsets,end_offsets))
        df.to_csv(f'{self.OUTDIR}/{self.OUTFILE}',header=False,index=False)
        if(df.shape[0]>0):
            print(f'\nTotal {df.shape[0]} messages written to {self.OUTDIR}/{self.OUTFILE}')
        return df.shape[0] ## return the count of records found
        
    def close(self):
        try:
            self.defconsumer.close()
        except Exception as e:
            print(e)

if __name__ == '__main__':
    ## Time mentioned is based on system TimeZone
    startts = os.environ.get('START_DATE', '2022-12-11T15:00:12')
    endts = os.environ.get('END_DATE', '2022-12-11T16:10:12')
    basedir=os.getcwd()
    propfile=f"{basedir}/my.prop"
    outdir=f"{basedir}/output"
    k = kafkautil(propfile,outdir)
    count = k.doWork(startts,endts)
    k.close()
