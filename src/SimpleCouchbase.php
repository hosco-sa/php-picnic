<?php

namespace Picnic;

class SimpleCouchbase {

    public $cluster;
    public $bucket;

    public function __construct($params)
    {
        $this->cluster = new \CouchbaseCluster('http://'.$params['host'].'?detailed_errcodes=1');

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

            foreach ($value as $k => $v) {
                if (is_string($v)) {
                    $value[$k] = utf8_encode($v);
                }
            }

            $json = json_encode($value);

            if (SimpleUtils::is_json($json)) {
                $res = $this->bucket->upsert($key, $value);

                return $res;
            }

        } catch (\CouchbaseException $e) {

            echo $e->getCode()."\n";
            echo $e->getMessage()."\n";

            return false;
        }
    }
}