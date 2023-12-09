```
# First create the model group to use
PUT /_cluster/settings
{
  "persistent" : {
    "plugins.ml_commons.model_access_control_enabled" : true 
  }
}

PUT /_cluster/settings
{
  "persistent" : {
    "plugins.ml_commons.only_run_on_ml_node" : false 
  }
}

PUT /_cluster/settings
{
  "persistent" : {
    "plugins.ml_commons.allow_registering_model_via_url" : true 
  }
}

PUT /_cluster/settings
{
  "persistent" : {
    "plugins.ml_commons.allow_registering_model_via_local_file" : true 
  }
}

# Or everything at the same time
PUT /_cluster/settings
{
  "persistent" : {
    "plugins.ml_commons.model_access_control_enabled" : true
    "plugins.ml_commons.only_run_on_ml_node" : false,
    "plugins.ml_commons.allow_registering_model_via_url" : true,
    "plugins.ml_commons.allow_registering_model_via_local_file" : true 
  }
}

POST /_plugins/_ml/model_groups/_register
{
    "name": "test_model_group_public",
    "description": "This is a public model group",
    "model_access_mode": "public"
}

# returned model_group_id = eRS4y4sBRT_C2Oekj20-

# register model
POST /_plugins/_ml/models/_register
{
  "name": "huggingface/sentence-transformers/all-MiniLM-L12-v2",
  "version": "1.0.1",
  "model_format": "TORCH_SCRIPT",
  "model_group_id": "eRS4y4sBRT_C2Oekj20-"
}
# returned task_id=exS6y4sBRT_C2OekYW2T

GET /_plugins/_ml/tasks/exS6y4sBRT_C2OekYW2T
# returned model_id=fBS6y4sBRT_C2Oeka22E

POST /_plugins/_ml/models/fBS6y4sBRT_C2Oeka22E/_load
# returned task_id=fRS7y4sBRT_C2Oekp20I

GET /_plugins/_ml/tasks/fRS7y4sBRT_C2Oekp20I

PUT _ingest/pipeline/nlp-pipeline
{
  "description": "An example neural search pipeline",
  "processors" : [
    {
      "text_embedding": {
        "model_id": "fBS6y4sBRT_C2Oeka22E",
        "field_map": {
           "passage_text": "passage_embedding"
        }
      }
    }
  ]
}

DELETE /scifact

PUT /scifact
{
    "settings": {
        "index.knn": true,
        "default_pipeline": "nlp-pipeline"
    },
    "mappings": {
        "properties": {
            "passage_embedding": {
                "type": "knn_vector",
                "dimension": 384,
                "method": {
                    "name":"hnsw",
                    "engine":"lucene",
                    "space_type": "l2",
                    "parameters":{
                        "m":16,
                        "ef_construction": 512
                    }
                }
            },
            "passage_text": { 
                "type": "text"            
            },
            "passage_key": { 
                "type": "text"            
            },
            "passage_title": { 
                "type": "text"            
            }
        }
    }
}

PUT /_search/pipeline/norm-minmax-pipeline
{
  "description": "Post processor for hybrid search",
  "phase_results_processors": [
    {
      "normalization-processor": {
        "normalization": {
          "technique": "min_max"
        },
        "combination": {
          "technique": "arithmetic_mean",
          "parameters": {
            "weights": [
              1.0
            ]
          }
        }
      }
    }
  ]
}

PUT /_search/pipeline/norm-minmax-pipeline-hybrid
{
  "description": "Post processor for hybrid search",
  "phase_results_processors": [
    {
      "normalization-processor": {
        "normalization": {
          "technique": "min_max"
        },
        "combination": {
          "technique": "arithmetic_mean",
          "parameters": {
            "weights": [
              0.4,
              0.3,
              0.3
            ]
          }
        }
      }
    }
  ]
}

GET scifact/_search?search_pipeline=norm-minmax-pipeline
```

Can also be created outside the console via curl
```bash
PORT=50365
HOST=localhost
URL="$HOST:$PORT"

curl -XPUT -H "Content-Type: application/json" $URL/_ingest/pipeline/nlp-pipeline -d '
{
  "description": "An example neural search pipeline",
  "processors" : [
    {
      "text_embedding": {
        "model_id": "AXA30IsByAqY8FkWHdIF",
        "field_map": {
           "passage_text": "passage_embedding"
        }
      }
    }
  ]
}'

curl -XDELETE $URL/$INDEX

curl -XPUT -H "Content-Type: application/json" $URL/scifact -d '
{
    "settings": {
        "index.knn": true,
        "default_pipeline": "nlp-pipeline"
    },
    "mappings": {
        "properties": {
            "passage_embedding": {
                "type": "knn_vector",
                "dimension": 384,
                "method": {
                    "name":"hnsw",
                    "engine":"lucene",
                    "space_type": "l2",
                    "parameters":{
                        "m":16,
                        "ef_construction": 512
                    }
                }
            },
            "passage_text": { 
                "type": "text"            
            },
            "passage_key": { 
                "type": "text"            
            },
            "passage_title": { 
                "type": "text"            
            }
        }
    }
}'

curl -XPUT -H "Content-Type: application/json" $URL/_search/pipeline/norm-minmax-pipeline-hybrid -d '
{
  "description": "Post processor for hybrid search",
  "phase_results_processors": [
    {
      "normalization-processor": {
        "normalization": {
          "technique": "min_max"
        },
        "combination": {
          "technique": "arithmetic_mean",
          "parameters": {
            "weights": [
              0.4,
              0.3,
              0.3
            ]
          }
        }
      }
    }
  ]
}'

curl -XPUT -H "Content-Type: application/json" $URL/_search/pipeline/norm-zscore-pipeline-hybrid -d '
{
  "description": "Post processor for hybrid search",
  "phase_results_processors": [
    {
      "normalization-processor": {
        "normalization": {
          "technique": "z_score"
        },
        "combination": {
          "technique": "arithmetic_mean",
          "parameters": {
            "weights": [
              0.4,
              0.3,
              0.3
            ]
          }
        }
      }
    }
  ]
}'

```

To use later with
```bash
PORT=50365
MODEL_ID="AXA30IsByAqY8FkWHdIF"
pipenv run python test_opensearch.py --dataset=scifact --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index="scifact" --operation=ingest
pipenv run python test_opensearch.py --dataset=scifact --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index="scifact" --operation=evaluate --method=bm25
pipenv run python test_opensearch.py --dataset=scifact --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index="scifact" --operation=evaluate --method=neural --pipelines=norm-minmax-pipeline --os_model_id=$MODEL_ID
pipenv run python test_opensearch.py --dataset=scifact --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index="scifact" --operation=evaluate --method=hybrid --pipelines=norm-minmax-pipeline-hybrid --os_model_id=$MODEL_ID
pipenv run python test_opensearch.py --dataset=scifact --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index="scifact" --operation=evaluate --method=hybrid --pipelines=norm-zscore-pipeline-hybrid --os_model_id=$MODEL_ID
```

To follow the Amazon approach
# Via the opensearch dashboard console
```
PUT /_cluster/settings
{
  "persistent" : {
    "plugins.ml_commons.model_access_control_enabled" : true,
    "plugins.ml_commons.only_run_on_ml_node" : false,
    "plugins.ml_commons.allow_registering_model_via_url" : true,
    "plugins.ml_commons.allow_registering_model_via_local_file" : true 
  }
}

POST /_plugins/_ml/model_groups/_register
{
    "name": "test_model_group_public",
    "description": "This is a public model group",
    "model_access_mode": "public"
}

# returned model_group_id = eRS4y4sBRT_C2Oekj20-
# register model
POST /_plugins/_ml/models/_register
{
  "name": "huggingface/sentence-transformers/msmarco-distilbert-base-tas-b",
  "version": "1.0.1",
  "model_format": "TORCH_SCRIPT",
  "model_group_id": "eRS4y4sBRT_C2Oekj20-"
}
# returned task_id=exS6y4sBRT_C2OekYW2T

GET /_plugins/_ml/tasks/exS6y4sBRT_C2OekYW2T
# returned model_id=fBS6y4sBRT_C2Oeka22E

POST /_plugins/_ml/models/fBS6y4sBRT_C2Oeka22E/_load
# returned task_id=fRS7y4sBRT_C2Oekp20I

GET /_plugins/_ml/tasks/fRS7y4sBRT_C2Oekp20I
# Wait until successful
```

# via shell
```bash
PORT=9200
HOST=localhost
URL="$HOST:$PORT"
INDEX="quora"
DATASET="quora"
MODEL_ID="dLrx6IsB4n5WT8oPiuAq"

curl -XPUT -H "Content-Type: application/json" $URL/_ingest/pipeline/nlp-pipeline -d '
{
  "description": "An example neural search pipeline",
  "processors" : [
    {
      "text_embedding": {
        "model_id": "dLrx6IsB4n5WT8oPiuAq",
        "field_map": {
           "passage_text": "passage_embedding"
        }
      }
    }
  ]
}'

curl -XDELETE $URL/$INDEX

curl -XPUT -H "Content-Type: application/json" $URL/$INDEX -d '
{
    "settings": {
        "index.knn": true,
        "default_pipeline": "nlp-pipeline",
        "number_of_shards": 4
    },
    "mappings": {
        "properties": {
            "passage_embedding": {
                "type": "knn_vector",
                "dimension": 768,
                "method": {
                    "name": "hnsw",
                    "engine": "nmslib",
                    "space_type": "innerproduct",
                    "parameters":{}
                }
            },
            "passage_text": {
                "type": "text"
            },
            "title_key": {
                "type": "text", "analyzer" : "english"
            },
            "text_key": {
                "type": "text", "analyzer" : "english"
            }
        }
    }
}'

curl -XPUT -H "Content-Type: application/json" $URL/_search/pipeline/norm-minmax-pipeline-hybrid -d '
{
  "description": "Post processor for hybrid search",
  "phase_results_processors": [
    {
      "normalization-processor": {
        "normalization": {
          "technique": "min_max"
        },
        "combination": {
          "technique": "arithmetic_mean"
        }
      }
    }
  ]
}'

curl -XPUT -H "Content-Type: application/json" $URL/_search/pipeline/norm-ltwo-pipeline-hybrid -d '
{
  "description": "Post processor for hybrid search",
  "phase_results_processors": [
    {
      "normalization-processor": {
        "normalization": {
          "technique": "l2"
        },
        "combination": {
          "technique": "arithmetic_mean"
        }
      }
    }
  ]
}'

curl -XPUT -H "Content-Type: application/json" $URL/_search/pipeline/norm-zscore-pipeline-hybrid -d '
{
  "description": "Post processor for hybrid search",
  "phase_results_processors": [
    {
      "normalization-processor": {
        "normalization": {
          "technique": "z_score"
        },
        "combination": {
          "technique": "arithmetic_mean"
        }
      }
    }
  ]
}'

curl -XPUT -H "Content-Type: application/json" $URL/_search/pipeline/norm-zscore--with-negatives-pipeline-hybrid -d '
{
  "description": "Post processor for hybrid search",
  "phase_results_processors": [
    {
      "normalization-processor": {
        "normalization": {
          "technique": "z_score"
        },
        "combination": {
          "technique": "arithmetic_mean_with_negatives_support"
        }
      }
    }
  ]
}'


pipenv run python test_opensearch.py --dataset=$DATASET --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index=$INDEX --operation=ingest
pipenv run python test_opensearch.py --dataset=$DATASET --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index=$INDEX  --operation=evaluate --method=bm25 > /tmp/bm25-results.log
pipenv run python test_opensearch.py --dataset=$DATASET --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index=$INDEX  --operation=evaluate --method=neural --pipelines=norm-minmax-pipeline-hybrid --os_model_id=$MODEL_ID > /tmp/neural-results.log
pipenv run python test_opensearch.py --dataset=$DATASET --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index=$INDEX  --operation=evaluate --method=hybrid --pipelines=norm-minmax-pipeline-hybrid --os_model_id=$MODEL_ID > /tmp/min-max-results.log
pipenv run python test_opensearch.py --dataset=$DATASET --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index=$INDEX  --operation=evaluate --method=hybrid --pipelines=norm-ltwo-pipeline-hybrid --os_model_id=$MODEL_ID > /tmp/l2-results.log
pipenv run python test_opensearch.py --dataset=$DATASET --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index=$INDEX  --operation=evaluate --method=hybrid --pipelines=norm-zscore-pipeline-hybrid --os_model_id=$MODEL_ID > /tmp/zscore-results.log
pipenv run python test_opensearch.py --dataset=$DATASET --dataset_url="https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip" --os_host=localhost --os_port=$PORT --os_index=$INDEX  --operation=evaluate --method=hybrid --pipelines=norm-zscore--with-negatives-pipeline-hybrid --os_model_id=$MODEL_ID > /tmp/zscore-with-negatives-results.log
```