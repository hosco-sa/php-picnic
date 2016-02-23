<?php

namespace Picnic;

class SimpleS3 {

    public $client;
    public $bucket;

    public function __construct($aws, $bucket=false)
    {
        $this->client = $aws->get('S3');
        $this->bucket = $bucket;
    }

    public function getBuckets()
    {
        $result = $this->client->listBuckets();
        return $result['Buckets'];
    }
}
