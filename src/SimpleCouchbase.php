<?php

namespace Picnic;

class SimpleCouchbase {

    public $cluster;
    public $bucket;

    public function __construct($params)
    {
        $this->cluster = new \CouchbaseCluster('couchbase://'.$params['host']);

        if ($params['bucket']) {
            $this->openBucket($params['bucket']);
        }
    }

    public function openBucket($bucket)
    {
        try {
            $this->bucket = $this->cluster->openBucket($bucket);

        } catch (\CouchbaseException $e) {
            echo $e->getCode()."\n";
            echo $e->getMessage()."\n";

            return false;
        }

    }

    public function getDocument($key)
    {
        try {

            $res = $this->bucket->get($key);

            return $res;

        } catch (\CouchbaseException $e) {

            echo $e->getCode()."\n";
            echo $e->getMessage()."\n";

            return false;

        }
    }

    public function setDocument($key, $value)
    {
        try {

            if ($key == 'ES_catalog_8007f54b') {
                print_r($value);

                foreach ($value as $k => $v) {
                    if (is_string($v)) {
                        $value[$k] = utf8_encode($v);
                    }
                }

                $json = json_encode($value);

                echo $json;

                if (SimpleUtils::is_json($json)) {
                    $res = $this->bucket->upsert($key, $value);

                    print_r($res);

                    die;
                }
            }

        } catch (\CouchbaseException $e) {

            echo $e->getCode()."\n";
            echo $e->getMessage()."\n";

            return false;
        }
    }
}