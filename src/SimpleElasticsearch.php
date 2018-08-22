<?php

namespace Picnic;

class SimpleElasticsearch {

    public $client;
    public $index;

    public function __construct($params)
    {
        $this->client = \Elasticsearch\ClientBuilder::create()->setHosts($params['hosts'])->build();
    }

    public function createIndex($index, $shards, $replicas)
    {
        $params = [
            'index' => $index,
            'body' => [
                'settings' => [
                    'number_of_shards' => $shards,
                    'number_of_replicas' => $replicas
                ]
            ]
        ];

        return $this->client->indices()->create($params);
    }

    public function deleteIndex($index)
    {
        $deleteParams = [
            'index' => $index
        ];

        return $this->client->indices()->delete($deleteParams);
    }

    public function index($index, $id, $body, $type=0)
    {
        $params = [
            'index' => $index,
            'type' => $type,
            'id' => $id,
            'body' => $body
        ];

        return $this->client->index($params);
    }

    public function bulk($index, $docs, $type=0, $extra=0)
    {
        $params = ['body' => []];

        $i = 0;

        foreach ($docs as $body) {
            $params['body'][] = [
                'index' => [
                    '_index' => $index,
                    '_type' => $type,
                    '_id' => md5($_SERVER['REMOTE_ADDR']."-".$extra."-".$body['ID_USER']),
                ]
            ];

            $params['body'][] = $body;

            // Every 100 documents stop and send the bulk request
            if ($i % 100 == 0) {
                $response = $this->client->bulk($params);

                // erase the old bulk request
                $params = ['body' => []];
            }

            $i++;
        }

        // Send the last batch if it exists
        if (!empty($params['body'])) {
            $response = $this->client->bulk($params);
        }

        return $response;
    }

    public function get($index, $id, $type=0)
    {
        $params = [
            'index' => $index,
            'type' => $type,
            'id' => $id
        ];

        return $this->client->get($params);
    }

    public function delete($index, $id, $type=0)
    {
        $params = [
            'index' => $index,
            'type' => $type,
            'id' => $id
        ];

        return $this->client->delete($params);
    }

    public function deleteByQuery($host, $index, $body, $type=0)
    {
        $endpoint = "$host/$index/$type/_delete_by_query";

        return SimpleUtils::curl_request($endpoint, 'POST', $body);
    }

    public function search($index, $body, $type=0)
    {
        $params = [
            'index' => $index,
            'type' => $type,
            'body' => $body
        ];

        return $this->client->search($params);
    }

    public function count($index, $body, $type=0)
    {
        $params = [
            'index' => $index,
            'type' => $type,
            'body' => $body
        ];

        return $this->client->count($params);
    }

    public function scan($index, $body, $size=100)
    {
        $params = [
            "search_type" => "scan",
            "scroll" => "10s",
            "size" => $size,
            "index" => $index,
            "body" => $body
        ];

        return $this->client->search($params);
    }

    public function scroll($scrollId)
    {
        $params = [
            "scroll_id" => "$scrollId",
            "scroll" => "10s"
        ];

        return $this->client->scroll($params);
    }

    public function truncateIndex($index)
    {
        $this->deleteIndex($index);

        return $this->createIndex($index, 5, 1);
    }

    public function curlRequest($endpoint, $method, $data)
    {
        print_r($this->client);

        $url = "http://".$host.":".$port."/".$this->app['elasticsearch_conn']['index']."/_search";

        return SimpleUtils::curl_request($endpoint, $method, $data);
    }
}