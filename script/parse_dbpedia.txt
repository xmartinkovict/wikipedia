REGISTER json-simple-1.1.1.jar
REGISTER piggybank.jar
REGISTER avro-1.7.7.jar

data = LOAD '/user/hue/DBpedia_data/sample_input_article_categories.ttl' 
			USING PigStorage(' ')
            AS (dbpedia_resource:chararray, subject:chararray, category:chararray, dot:chararray);
            
data1 = FOREACH data GENERATE dbpedia_resource, category;
data2 = FOREACH data1 GENERATE 
			REGEX_EXTRACT(dbpedia_resource, '<http://sk.dbpedia.org/resource/(.*)>', 1) as (dbpedia_resource:chararray),
            REGEX_EXTRACT(category, '<http://sk.dbpedia.org/resource/Kategória:(.*)>', 1) as (category:chararray);

dbpedia_resources = DISTINCT (FOREACH data2 GENERATE dbpedia_resource);

--aaa = FOREACH dbpedia_resources GENERATE TOTUPLE(dbpedia_resource);


data3 = GROUP data2 BY dbpedia_resource;

data4 = FOREACH data3 {
			category_column = FOREACH data2 GENERATE category;
        	GENERATE group AS dbpedia_resource, category_column AS category;
      	}


--DESCRIBE data;
--DESCRIBE data1;
--DESCRIBE data2;
--ILLUSTRATE data2;
--dump data2;
--DESCRIBE dbpedia_resources;
--dump dbpedia_resources;
--dump aaa;

DESCRIBE data3;
DESCRIBE data4;
dump data3;
dump data4;

--STORE data4 INTO '/user/hue/DBpedia_data/output' USING PigStorage (',');

STORE data4 INTO '/user/hue/DBpedia_data/output1' USING org.apache.pig.piggybank.storage.avro.AvroStorage(
		'{"schema": {
                "type": "record",
				"namespace": "sk.stuba.fiit.vi",
				"name": "DBPedia",
				"fields": [
							{"name": "dbpedia_resource", "type": "string"},
							{"name": "category", "type": [{"type":"array", "items":"string"}, "null"]}
						]
                }
		  }'
);
