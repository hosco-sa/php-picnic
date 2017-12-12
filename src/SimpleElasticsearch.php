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
}