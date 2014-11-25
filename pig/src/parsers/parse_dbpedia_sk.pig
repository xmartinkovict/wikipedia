REGISTER json-simple-1.1.1.jar
REGISTER piggybank.jar
REGISTER avro-1.7.7.jar

data = LOAD '$input' AS (data_all:chararray);

data = FOREACH data GENERATE 
			REGEX_EXTRACT(data_all, '^([^#].*)', 1) AS data_all;
            
data = FILTER data BY data_all is not null;

--DESCRIBE data;
--dump data;


--#############################################################
--PARSE ARTICLE CATEGORIES
--#############################################################

data1 = FOREACH data GENERATE 
		REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://purl.org/dc/terms/subject>\\s<http://sk.dbpedia.org/resource/Kategória:.*>\\s.$', 1)
            		AS dbpedia_resource,
        REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/.*>\\s<http://purl.org/dc/terms/subject>\\s<http://sk.dbpedia.org/resource/Kategória:(.*)>\\s.$', 1) 
            		AS category;
                    
data1 = FILTER data1 BY dbpedia_resource is not null;

data_group = GROUP data1 BY dbpedia_resource;

data_categories = FOREACH data_group{
						category_column = FOREACH data1 GENERATE category;
        				GENERATE group AS dbpedia_resource, category_column AS category;
      				}

--DESCRIBE data_categories;
--dump data_categories;




--#############################################################
--PARSE ARTICLE TEMPLATES
--#############################################################

data1 = FOREACH data GENERATE 
		REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://sk.dbpedia.org/property/wikiPageUsesTemplate>\\s<http://sk.dbpedia.org/resource/Šablóna:.*>\\s.$', 1)
            		AS dbpedia_resource,
        REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/.*>\\s<http://sk.dbpedia.org/property/wikiPageUsesTemplate>\\s<http://sk.dbpedia.org/resource/Šablóna:(.*)>\\s.$', 1) 
            		AS template;
                    
data1 = FILTER data1 BY dbpedia_resource is not null;

data_group = GROUP data1 BY dbpedia_resource;

data_templates = FOREACH data_group {
						template_column = FOREACH data1 GENERATE template;
        				GENERATE group AS dbpedia_resource, template_column AS template;
      				}

--DESCRIBE data_templates;
--dump data_templates;




--#############################################################
--PARSE INFOBOX PROPERTIES
--#############################################################

data1 = FOREACH data GENERATE 
			REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://sk.dbpedia.org/property/.*>\\s.*\\s.$', 1) 
            				AS dbpedia_resource,
            REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/.*>\\s<http://sk.dbpedia.org/property/(.*)>\\s.*\\s.$', 1) 
            				AS property,
            REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/.*>\\s<http://sk.dbpedia.org/property/.*>\\s(.*)\\s.$', 1) 
            				AS infobox_property;

data1 = FILTER data1 BY dbpedia_resource is not null;
data1 = FILTER data1 BY property != 'wikiPageUsesTemplate';

data1 = FOREACH data1 GENERATE dbpedia_resource, property,
			(REGEX_EXTRACT(infobox_property, '"(.*)"@sk', 1) IS NOT NULL ? REGEX_EXTRACT(infobox_property, '"(.*)"@sk', 1) : infobox_property) AS infobox_property;

data_group = GROUP data1 BY dbpedia_resource;

data_infobox_property = FOREACH data_group{
							column = FOREACH data1 GENERATE property, infobox_property;
        					GENERATE group AS dbpedia_resource, column AS infobox_property;
      					}

--DESCRIBE data_infobox_property;
--dump data_infobox_property;




--#############################################################
--PARSE INSTANCE TYPES
--#############################################################

data1 = FOREACH data GENERATE 
		REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\\s<.*>\\s.$', 1)
            		AS dbpedia_resource,
        REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/.*>\\s<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>\\s<(.*)>\\s.$', 1) 
            		AS instance_type;
                    
data1 = FILTER data1 BY dbpedia_resource is not null;

data_group = GROUP data1 BY dbpedia_resource;

data_instance_type = FOREACH data_group{
							instance_type_column = FOREACH data1 GENERATE instance_type;
        					GENERATE group AS dbpedia_resource, instance_type_column AS instance_type;
      					}

--DESCRIBE data_instance_type;
--dump data_instance_type;




--#############################################################
--PARSE LONG ABSTRACTS
--#############################################################

data1 = FOREACH data GENERATE 
			REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://dbpedia.org/ontology/abstract>\\s".*"@sk\\s.$', 1) 
            				AS dbpedia_resource,
            REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/.*>\\s<http://dbpedia.org/ontology/abstract>\\s"(.*)"@sk\\s.$', 1) 
            				AS long_abstract;

data1 = FILTER data1 BY dbpedia_resource is not null;

data_group = GROUP data1 BY dbpedia_resource;

data_long_abstract = FOREACH data_group{
						long_abstract_column = FOREACH data1 GENERATE long_abstract;
        				GENERATE group AS dbpedia_resource, long_abstract_column AS long_abstract;
      				}

--DESCRIBE data_long_abstract;
--dump data_long_abstract;




--#############################################################
--PARSE SHORT ABSTRACTS
--#############################################################

data1 = FOREACH data GENERATE 
			REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://www.w3.org/2000/01/rdf-schema#comment>\\s".*"@sk\\s.$', 1) 
            				AS dbpedia_resource,
            REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/.*>\\s<http://www.w3.org/2000/01/rdf-schema#comment>\\s"(.*)"@sk\\s.$', 1) 
            				AS short_abstract;

data1 = FILTER data1 BY dbpedia_resource is not null;

data_group = GROUP data1 BY dbpedia_resource;

data_short_abstract = FOREACH data_group{
						short_abstract_column = FOREACH data1 GENERATE short_abstract;
        				GENERATE group AS dbpedia_resource, short_abstract_column AS short_abstract;
      				}

--DESCRIBE data_short_abstract;
--dump data_short_abstract;




--#############################################################
--PARSE WIKIPEDIA LINKS
--#############################################################

data1 = FOREACH data GENERATE 
		REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/(.*)>\\s<http://xmlns.com/foaf/0.1/isPrimaryTopicOf>\\s<http://sk.wikipedia.org/wiki/.*>\\s.$', 1)
            		AS dbpedia_resource,
        REGEX_EXTRACT(data_all, '^<http://sk.dbpedia.org/resource/.*>\\s<http://xmlns.com/foaf/0.1/isPrimaryTopicOf>\\s<(http://sk.wikipedia.org/wiki/.*)>\\s.$', 1) 
            		AS wikipedia_link;
                    
data1 = FILTER data1 BY dbpedia_resource is not null;

data_group = GROUP data1 BY dbpedia_resource;

data_wikipedia_link = FOREACH data_group{
							wikipedia_link_column = FOREACH data1 GENERATE wikipedia_link;
        					GENERATE group AS dbpedia_resource, wikipedia_link_column AS wikipedia_link;
      					}
                        
--DESCRIBE data_wikipedia_link;
--dump data_wikipedia_link;




--#############################################################
--STORE INTO AVRO
--#############################################################

data_all = JOIN data_categories BY dbpedia_resource FULL OUTER, data_templates BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_categories::dbpedia_resource IS NOT NULL ? data_categories::dbpedia_resource : data_templates::dbpedia_resource) AS dbpedia_resource,
									 data_categories::category AS category,
                                     data_templates::template AS template;


data_all = JOIN data_infobox_property BY dbpedia_resource FULL OUTER, data_all BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_all::dbpedia_resource IS NOT NULL ? data_all::dbpedia_resource : data_infobox_property::dbpedia_resource) AS dbpedia_resource,
									 data_all::category AS category,
                                     data_all::template AS template,
                                     data_infobox_property::infobox_property AS infobox_property;


data_all = JOIN data_instance_type BY dbpedia_resource FULL OUTER, data_all BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_all::dbpedia_resource IS NOT NULL ? data_all::dbpedia_resource : data_instance_type::dbpedia_resource) AS dbpedia_resource,
									 data_all::category AS category,
                                     data_all::template AS template,
                                     data_all::infobox_property AS infobox_property,
                                     data_instance_type::instance_type AS instance_type;
                                     
                                     
data_all = JOIN data_long_abstract BY dbpedia_resource FULL OUTER, data_all BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_all::dbpedia_resource IS NOT NULL ? data_all::dbpedia_resource : data_long_abstract::dbpedia_resource) AS dbpedia_resource,
									 data_all::category AS category,
                                     data_all::template AS template,
                                     data_all::infobox_property AS infobox_property,
                                     data_all::instance_type AS instance_type,
                                     data_long_abstract::long_abstract AS long_abstract;
                                     
                                     
data_all = JOIN data_short_abstract BY dbpedia_resource FULL OUTER, data_all BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_all::dbpedia_resource IS NOT NULL ? data_all::dbpedia_resource : data_short_abstract::dbpedia_resource) AS dbpedia_resource,
									 data_all::category AS category,
                                     data_all::template AS template,
                                     data_all::infobox_property AS infobox_property,
                                     data_all::instance_type AS instance_type,
                                     data_all::long_abstract AS long_abstract,
                                     data_short_abstract::short_abstract AS short_abstract;
                                     
                                     
data_all = JOIN data_wikipedia_link BY dbpedia_resource FULL OUTER, data_all BY dbpedia_resource;
data_all = FOREACH data_all GENERATE (data_all::dbpedia_resource IS NOT NULL ? data_all::dbpedia_resource : data_wikipedia_link::dbpedia_resource) AS dbpedia_resource,
									 data_all::category AS category,
                                     data_all::template AS template,
                                     data_all::infobox_property AS infobox_property,
                                     data_all::instance_type AS instance_type,
                                     data_all::long_abstract AS long_abstract,
                                     data_all::short_abstract AS short_abstract,
                                     data_wikipedia_link::wikipedia_link AS wikipedia_link;

--DESCRIBE data_all;
--dump data_all;

STORE data_all INTO '$output' USING org.apache.pig.piggybank.storage.avro.AvroStorage(
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
                            {"name": "short_abstract", "type": [{"type":"array", "items":"string"}, "null"]},
                            {"name": "wikipedia_link", "type": [{"type":"array", "items":"string"}, "null"]}
                            
                            
						]
                }
		  }'
);