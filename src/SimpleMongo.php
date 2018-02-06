<?php

namespace Picnic;

use MongoDB;

class SimpleMongo {

    public $params;
    public $uri;
    public $collection;

    private $threshold = 100;

    public function __construct($params, $database=null)
    {
        $this->params = $params;

        try {
            $this->client = new MongoDB\Client($params['uri']);

            if ($database) {
                $this->client->selectDatabase($database);
            }

        } catch (Exception $e) {
            return $e->getMessage();
        }

        return $this;
    }

    public function listDatabases()
    {
        try {
            $databases = $this->client->listDatabases();

            foreach ($databases as $database) {
                $d['name'] = $database->getName();
                $d['size_on_disk'] = $database->getSizeOnDisk();

                $arDatabases[] = $d;
            }

        } catch (Exception $e) {
            echo $e->getMessage();
        }

        return $arDatabases;
    }

    public function listCollections($database)
    {
        try {
            $db = $this->client->selectDatabase($database);
            $collections = $db->listCollections();

            foreach ($collections as $collection) {
                $c['name'] = $collection->getName();

                $arCollections[] = $c;
            }
        } catch (Exception $e) {
            echo $e->getMessage();
        }

        return $arCollections;
    }

    public function collectionCount($collection)
    {
        try {
            $count = $collection->count();

        } catch (Exception $e) {
            echo $e->getMessage();
        }

        return $count;
    }

    public function getByAttVal($database, $collection, $att, $val)
    {
        try {
            $doc = $this->client->$database->$collection->findOne([$att => $val]);

        } catch (Exception $e) {
            echo $e->getMessage();
        }

        return $doc;
    }
}
