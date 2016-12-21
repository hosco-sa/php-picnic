<?php

namespace Picnic;

class SimpleCouchbase {

    public $cluster;
    public $bucket;

    public function __construct($params)
    {
        $this->cluster = new \CouchbaseCluster("couchbase://".$params['host']);

        if ($params['bucket']) {
            $this->bucket = $params['bucket'];
            $this->cluster->openBucket($this->bucket);
        }
    }

    public function createBucket($bucket, $attributes)
    {
        try {
            $this->cluster->createBucket($bucket, $attributes);

        } catch (\CouchbaseException $e) {
            echo $e->getCode()."\n";
            echo $e->getMessage()."\n";

            return false;
        }

    }

    public function getDocument($key)
    {
        try {

            $res = $this->cluster->openBucket($this->bucket)->get($key);
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

            foreach ($value as $k => $v) {
                if (is_string($v)) {
                    if (mb_detect_encoding($v) != 'UTF-8') {
                        $value[$k] = utf8_encode($v);
                    }
                }
            }

            $json = json_encode($value);

            if (SimpleUtils::is_json($json)) {

                $res = $this->cluster->openBucket($this->bucket)->upsert($key, $value);

                return $res;
            }

        } catch (\CouchbaseException $e) {

            echo $e->getCode()."\n";
            echo $e->getMessage()."\n";

            return false;
        }
    }
}