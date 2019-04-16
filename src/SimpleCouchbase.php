<?php

namespace Picnic;

use CouchbaseN1qlQuery;
use CouchbaseException;
use ElasticSearch\Exception;

class SimpleCouchbase {

    public $cluster;
    public $bucket;

    /**
     * CONSTRUCTOR
     *
     * @param $params
     */
    public function __construct($params)
    {
        $this->cluster = new \CouchbaseCluster("couchbase://{$params['host']}?operation_timeout=5");
        $this->cluster->authenticateAs($params['user'], $params['pass']);

        if ($params['bucket']) {
            $this->bucket = $params['bucket'];
            $this->cluster->openBucket($this->bucket);
        }
    }

    /**
     * CREATE Bucket
     *
     * @param $bucket
     * @param $attributes
     * @return bool
     */
    public function createBucket($bucket, $attributes)
    {
        try {
            $this->cluster->createBucket($bucket, $attributes);

        } catch (\CouchbaseException $e) {
            // echo $e->getCode()." ".$e->getMessage()."\n";
            return false;

        }

    }

    /**
     * GET Document
     *
     * @param $key
     * @return array|bool|\Couchbase\Document
     */
    public function getDocument($key)
    {
        try {
            $res = $this->cluster->openBucket($this->bucket)->get($key);
            return $res;

        } catch (\CouchbaseException $e) {
            // echo $e->getCode()." ".$e->getMessage()."\n";
            return false;

        }
    }

    /**
     * SET Document
     *
     * @param $key
     * @param $value
     * @param array $options
     * @return array|bool|\Couchbase\Document
     */
    public function setDocument($key, $value, $options=[])
    {
        try {

            if (is_array($value)) {
                foreach ($value as $k => $v) {
                    if (is_string($v)) {
                        if (mb_detect_encoding($v) != 'UTF-8') {
                            $value[$k] = utf8_encode($v);
                        }
                    }
                }
            }

            $json = json_encode($value);

            if (SimpleUtils::is_json($json)) {

                $res = $this->cluster->openBucket($this->bucket)->upsert($key, $value, $options);

                return $res;
            }

        } catch (\CouchbaseException $e) {
            // echo $e->getCode()." ".$e->getMessage()."\n";
            return false;

        }
    }

    /**
     * SET Partial Document
     *
     * @param $key
     * @param $value
     * @return array|bool|\Couchbase\Document
     */
    public function setPartialDocument($key, $value)
    {
        try {

            $res = $this->cluster->openBucket($this->bucket)->get($key);

            if (is_object($res->value)) {
                $arOld = (array) $res->value;
            } else {
                $arOld = json_decode($res->value, true);
            }

            $arNew = json_decode($value, true);

            // print_r($arOld);

            if (isset($arNew['uuid'])) unset($arNew['uuid']);
            if (isset($arNew['entity'])) unset($arNew['entity']);

            // print_r($arNew);

            $arMerged = array_merge($arOld, $arNew);

            // print_r($arMerged);

            $json = stripslashes(json_encode($arMerged));

            if (SimpleUtils::is_json($json)) {
                $res = $this->cluster->openBucket($this->bucket)->upsert($key, $json);

                if (!$res->error) {
                    return $res;
                }
            }

        } catch (\CouchbaseException $e) {
            // echo $e->getCode()." ".$e->getMessage()."\n";
            return false;

        }
    }

    /**
     * DELETE Document
     *
     * @param $key
     * @return array|bool|\Couchbase\Document
     */
    public function delDocument($key)
    {
        try {
            $res = $this->cluster->openBucket($this->bucket)->remove($key);

            if (!$res->error) {
                return $res;
            }

        } catch (\CouchbaseException $e) {
            // echo $e->getCode()." ".$e->getMessage()."\n";
            return false;

        }
    }

    /**
     * CREATE PRIMARY INDEX
     *
     * @param string $index
     * @param string $bucket
     */
    public function createPrimaryIndex($index='primary_index', $bucket='default')
    {
        try {
            $queryI = CouchbaseN1qlQuery::fromString("CREATE PRIMARY INDEX `$index` ON `$bucket` USING GSI;");

            $resultI = $this->cluster->openBucket($this->bucket)->query($queryI);

            print_r($resultI);

        } catch (CouchbaseException $e) {
            echo $e->getMessage()."\n";
        }
    }

    /**
     * DUMP ALL KEYS
     *
     * @param $bucket
     * @param int $limit
     * @param int $offset
     * @param string $id
     * @param string $folder
     */
    public function dumpAllKeys($bucket, $limit=1000, $offset=0, $id='uuid', $folder='storage')
    {
        try {
            $queryS = CouchbaseN1qlQuery::fromString("SELECT * FROM `$bucket` LIMIT $limit OFFSET $offset");

            $resultS = $this->cluster->openBucket($this->bucket)->query($queryS);

            $n = 0;

	    // print_r($resultS); die;

	    if (isset($resultS->rows)) {
		$resultS = $resultS->rows;
	    }

            foreach ($resultS as $row) {

                if (isset($row->$bucket->$id)) {
                    $json = json_encode($row->$bucket, JSON_UNESCAPED_SLASHES);

                    echo $n++.": ".$row->$bucket->$id."\n";

                    file_put_contents($folder.'/'.$row->$bucket->$id.'.json', $json);
                } else {
                    $json = json_encode($row, JSON_UNESCAPED_SLASHES);

                    echo $json."\n";
                }
            }

        } catch (CouchbaseException $e) {
            echo $e->getMessage()."\n";
        }
    }

    /**
     * DUMP ALL KEYS w/ prefix
     *
     * @param $bucket
     * @param int $limit
     * @param int $offset
     * @param string $id
     * @param string $folder
     * @param string $prefix
     */
    public function dumpAllKeysWithPrefix($bucket, $limit=1000, $offset=0, $id='uuid', $folder='storage', $prefix='0')
    {
        try {
            $queryS = CouchbaseN1qlQuery::fromString("SELECT * FROM `$bucket` WHERE $id LIKE '$prefix%' LIMIT $limit OFFSET $offset");

            $resultS = $this->cluster->openBucket($this->bucket)->query($queryS);

            $n = 0;

            // print_r($resultS); die;

            if (isset($resultS->rows)) {
                $resultS = $resultS->rows;
            }

            foreach ($resultS as $row) {

                if (isset($row->$bucket->$id)) {
                    $json = json_encode($row->$bucket, JSON_UNESCAPED_SLASHES);

                    echo $n++.": ".$row->$bucket->$id."\n";

                    file_put_contents($folder.'/'.substr($row->$bucket->$id,0,1).'/'.$row->$bucket->$id.'.json', $json);
                } else {
                    $json = json_encode($row, JSON_UNESCAPED_SLASHES);

                    echo $json."\n";
                }
            }

        } catch (CouchbaseException $e) {
            echo $e->getMessage()."\n";
        }
    }
}
