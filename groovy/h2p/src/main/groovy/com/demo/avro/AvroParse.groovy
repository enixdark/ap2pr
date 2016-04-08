package com.demo.avro

import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord

class AvroParse {
	static def genericRecord(){
		try{
			def schema = new Schema.Parser().parse(new File("resources/tweets_avro.avsc"))
			def tweet = new GenericData.Record(schema)
			tweet.put("text", "The generic tweet text")
			def file = new File("resources/tweets.arvo")
	
			def datumWriter = new GenericDatumWriter<>(schema)
			def fileWriter = new DataFileWriter<>(datumWriter)
			fileWriter.create(schema,file).append(tweet)
			fileWriter.close()
			def datumReader = new GenericDatumReader<>(schema)
			def fileReader = new DataFileReader<>(file, datumReader)
			
			def genericTweet = null
			while(fileReader.hasNext()){
				genericTweet = (GenericRecord) fileReader.next(genericTweet)
				genericTweet.getSchema().getFields().each {
					 field ->
						 def val = genericTweet.get(field.name())
						 if (val != null){
							 println val
						 }
				}
			}
			
		}
		catch(e){
			e.printStackTrace()
		}
		

	}
}
