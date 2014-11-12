REGISTER json-simple-1.1.1.jar
REGISTER piggybank.jar
REGISTER avro-1.7.7.jar



--#############################################################
--PARSE ARTICLE CATEGORIES
--#############################################################
data = LOAD '/user/hue/DBpedia_data/input/article_categories_sk.ttl' 
			USING PigStorage(' ')
            AS (dbpedia_resource:chararray, subject:chararray, category:chararray, dot:chararray);
            
data = FILTER data BY dbpedia_resource != '#';
            
data = FOREACH data GENERATE dbpedia_resource, category;
data = FOREACH data GENERATE 
			REGEX_EXTRACT(dbpedia_resource, '<http://sk.dbpedia.org/resource/(.*)>', 1) as (dbpedia_resource:chararray),
            REGEX_EXTRACT(category, '<http://sk.dbpedia.org/resource/Kategória:(.*)>', 1) as (category:chararray);

data_group = GROUP data BY dbpedia_resource;

data_categories = FOREACH data_group{
						category_column = FOREACH data GENERATE category;
        				GENERATE group AS dbpedia_resource, category_column AS category;
      				}

/*DESCRIBE data_categories;
dump data_categories;*/




--#############################################################
--PARSE ARTICLE TEMPLATES
--#############################################################
data = LOAD '/user/hue/DBpedia_data/input/article_templates_sk.ttl' 
			USING PigStorage(' ')
            AS (dbpedia_resource:chararray, wikiPageUsesTemplate:chararray, template:chararray, dot:chararray);

data = FILTER data BY dbpedia_resource != '#';

data = FOREACH data GENERATE dbpedia_resource, template;

data = FOREACH data GENERATE 
			REGEX_EXTRACT(dbpedia_resource, '<http://sk.dbpedia.org/resource/(.*)>', 1) as (dbpedia_resource:chararray),
            REGEX_EXTRACT(template, '<http://sk.dbpedia.org/resource/Šablóna:(.*)>', 1) as (template:chararray);

data_group = GROUP data BY dbpedia_resource;

data_templates = FOREACH data_group {
			template_column = FOREACH data GENERATE template;
        	GENERATE group AS dbpedia_resource, template_column AS template;
      	}

/*DESCRIBE data_templates;
dump data_templates;*/




--#############################################################
--PARSE INFOBOX PROPERTIES
--#############################################################
data = LOAD '/user/hue/DBpedia_data/input/infobox_properties_sk.ttl'
			AS (infobox_property:chararray);

data = FOREACH data GENERATE 
			REGEX_EXTRACT(infobox_property, '^([^#].*)', 1) AS infobox_property;
            
data = FILTER data BY infobox_property is not null;

data = FOREACH data GENERATE 
			REGEX_EXTRACT(infobox_property, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://sk.dbpedia.org/property/.*>\\s.*\\s.$', 1) 
            				AS dbpedia_resource,
            REGEX_EXTRACT(infobox_property, '^<http://sk.dbpedia.org/resource/.*>\\s<http://sk.dbpedia.org/property/(.*)>\\s.*\\s.$', 1) 
            				AS property,
            REGEX_EXTRACT(infobox_property, '^<http://sk.dbpedia.org/resource/.*>\\s<http://sk.dbpedia.org/property/.*>\\s(.*)\\s.$', 1) 
            				AS infobox_property;

data = FOREACH data GENERATE dbpedia_resource, property,
			(REGEX_EXTRACT(infobox_property, '"(.*)"@sk', 1) IS NOT NULL ? REGEX_EXTRACT(infobox_property, '"(.*)"@sk', 1) : infobox_property) AS infobox_property;

data_group = GROUP data BY dbpedia_resource;

data_infobox_property = FOREACH data_group{
							column = FOREACH data GENERATE property, infobox_property;
        					GENERATE group AS dbpedia_resource, column AS infobox_property;
      					}

/*DESCRIBE data_infobox_property;
dump data_infobox_property;*/




--#############################################################
--PARSE INSTANCE TYPES
--#############################################################
data = LOAD '/user/hue/DBpedia_data/input/instance_types_sk.ttl'
			USING PigStorage(' ')
            AS (dbpedia_resource:chararray, type:chararray, instance_type:chararray, dot:chararray);

data = FILTER data BY dbpedia_resource != '#';

data = FOREACH data GENERATE 
			REGEX_EXTRACT(dbpedia_resource, '<http://sk.dbpedia.org/resource/(.*)>', 1) as dbpedia_resource, instance_type;

data_group = GROUP data BY dbpedia_resource;

data_instance_type = FOREACH data_group{
							instance_type_column = FOREACH data GENERATE instance_type;
        					GENERATE group AS dbpedia_resource, instance_type_column AS instance_type;
      					}

/*DESCRIBE data_instance_type;
dump data_instance_type;*/




--#############################################################
--PARSE LONG ABSTRACTS
--#############################################################
data = LOAD '/user/hue/DBpedia_data/input/long_abstracts_sk.ttl'
			AS (long_abstract:chararray);

data = FOREACH data GENERATE 
			REGEX_EXTRACT(long_abstract, '^([^#].*)', 1) AS long_abstract;
            
data = FILTER data BY long_abstract is not null;

data = FOREACH data GENERATE 
			REGEX_EXTRACT(long_abstract, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://dbpedia.org/ontology/abstract>\\s".*"@sk\\s.', 1) 
            				AS dbpedia_resource,
            REGEX_EXTRACT(long_abstract, '^<http://sk.dbpedia.org/resource/.*>\\s<http://dbpedia.org/ontology/abstract>\\s"(.*)"@sk\\s.', 1) 
            				AS long_abstract;

data_group = GROUP data BY dbpedia_resource;

data_long_abstract = FOREACH data_group{
						long_abstract_column = FOREACH data GENERATE long_abstract;
        				GENERATE group AS dbpedia_resource, long_abstract_column AS long_abstract;
      				}

/*DESCRIBE data_long_abstract;
dump data_long_abstract;*/




--#############################################################
--PARSE SHORT ABSTRACTS
--#############################################################
data = LOAD '/user/hue/DBpedia_data/input/short_abstracts_sk.ttl'
			AS (short_abstract:chararray);

data = FOREACH data GENERATE 
			REGEX_EXTRACT(short_abstract, '^([^#].*)', 1) AS short_abstract;
            
data = FILTER data BY short_abstract is not null;

data = FOREACH data GENERATE 
			REGEX_EXTRACT(short_abstract, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://www.w3.org/2000/01/rdf-schema#comment>\\s".*"@sk\\s.', 1) 
            				AS dbpedia_resource,
            REGEX_EXTRACT(short_abstract, '^<http://sk.dbpedia.org/resource/.*>\\s<http://www.w3.org/2000/01/rdf-schema#comment>\\s"(.*)"@sk\\s.', 1) 
            				AS short_abstract;

data_group = GROUP data BY dbpedia_resource;

data_short_abstract = FOREACH data_group{
						short_abstract_column = FOREACH data GENERATE short_abstract;
        				GENERATE group AS dbpedia_resource, short_abstract_column AS short_abstract;
      				}

/*DESCRIBE data_short_abstract;
dump data_short_abstract;*/




--#############################################################
--STORE INTO AVRO
--#############################################################

data_all = JOIN data_categories BY dbpedia_resource FULL OUTER, data_templates BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_categories::dbpedia_resource IS NOT NULL ? data_categories::dbpedia_resource : data_templates::dbpedia_resource) AS dbpedia_resource,
									 data_categories::category AS category,
                                     data_templates::template AS template;


data_all = JOIN data_all BY dbpedia_resource FULL OUTER, data_infobox_property BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_all::dbpedia_resource IS NOT NULL ? data_all::dbpedia_resource : data_infobox_property::dbpedia_resource) AS dbpedia_resource,
									 data_all::category AS category,
                                     data_all::template AS template,
                                     data_infobox_property::infobox_property AS infobox_property;


data_all = JOIN data_all BY dbpedia_resource FULL OUTER, data_instance_type BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_all::dbpedia_resource IS NOT NULL ? data_all::dbpedia_resource : data_instance_type::dbpedia_resource) AS dbpedia_resource,
									 data_all::category AS category,
                                     data_all::template AS template,
                                     data_all::infobox_property AS infobox_property,
                                     data_instance_type::instance_type AS instance_type;
                                     
                                     
data_all = JOIN data_all BY dbpedia_resource FULL OUTER, data_long_abstract BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_all::dbpedia_resource IS NOT NULL ? data_all::dbpedia_resource : data_long_abstract::dbpedia_resource) AS dbpedia_resource,
									 data_all::category AS category,
                                     data_all::template AS template,
                                     data_all::infobox_property AS infobox_property,
                                     data_all::instance_type AS instance_type,
                                     data_long_abstract::long_abstract AS long_abstract;
                                     
                                     
data_all = JOIN data_all BY dbpedia_resource FULL OUTER, data_short_abstract BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_all::dbpedia_resource IS NOT NULL ? data_all::dbpedia_resource : data_short_abstract::dbpedia_resource) AS dbpedia_resource,
									 data_all::category AS category,
                                     data_all::template AS template,
                                     data_all::infobox_property AS infobox_property,
                                     data_all::instance_type AS instance_type,
                                     data_all::long_abstract AS long_abstract,
                                     data_short_abstract::short_abstract AS short_abstract;

DESCRIBE data_all;
dump data_all;

STORE data_all INTO '/user/hue/DBpedia_data/output/output' USING org.apache.pig.piggybank.storage.avro.AvroStorage(
		'{"schema": {
                "type": "record",
				"namespace": "sk.stuba.fiit.vi",
				"name": "DBPedia",
				"fields": [
							{"name": "dbpedia_resource", "type": "string"},
                            {"name": "category", "type": [{"type":"array", "items":"string"}, "null"]},
							{"name": "template", "type": [{"type":"array", "items":"string"}, "null"]},
                            {"name": "infobox_property", "type": [{"type":"array", "items":{
                            					"name":"infobox_property",
                            					"type":"record",
                                                "fields":[
                                                   			{"name": "property", "type": "string"},
                            					 			{"name": "infobox_property", "type": "string"}
                                                         ]
                                                 										  }}, "null"]},
                            {"name": "instance_type", "type": [{"type":"array", "items":"string"}, "null"]},
                            {"name": "long_abstract", "type": [{"type":"array", "items":"string"}, "null"]},
                            {"name": "short_abstract", "type": [{"type":"array", "items":"string"}, "null"]}
                            
                            
						]
                }
		  }'
);
