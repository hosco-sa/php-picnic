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

    public function createBucket($bucket)
    {
        $result = $this->client->createBucket(array( 'Bucket' => $bucket, 'LocationConstraint' => 'eu-west-1' ));
        return $result;
    }

    public function pollBucket($bucket)
    {
        $result = $this->client->waitUntil('BucketExists', array('Bucket' => $bucket, 'LocationConstraint' => 'eu-west-1',));
        return $result;
    }

    public function deleteBucket($bucket)
    {

    }

    public function putObject($bucket, $key, $body)
    {
        // Upload an object to Amazon S3
        $result = $this->client->putObject(array(
            'Bucket' => $bucket,
            'Key'    => $key,
            'Body'   => $body
        ));

        return $result;
    }

    public function putImage($bucket, $key, $file, $content_type='image/jpeg')
    {
        // Upload an image to Amazon S3
        $result = $this->client->putObject(array(
            'Bucket' => $bucket,
            'Key'    => $key,
            'SourceFile' => $file,
            'ContentType' => $content_type,
            'ACL'          => 'public-read',
            'StorageClass' => 'REDUCED_REDUNDANCY'
        ));

        return $result;
    }

    public function pollObject($bucket, $key)
    {
        // We can poll the object until it is accessible
        $res = $this->client->waitUntil('ObjectExists', array(
            'Bucket' => $bucket,
            'Key'    => $key
        ));

        return $res;
    }

}
