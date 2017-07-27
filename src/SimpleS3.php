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

    public function copyObject($bucketOrigin, $keyOrigin, $bucketDestination, $keyDestination)
    {
        // Copy an object
        $result = $this->client->copyObject(array(
            'Bucket'     => $bucketDestination,
            'Key'        => $keyDestination,
            'CopySource' => "{$bucketOrigin}/{$keyOrigin}",
        ));
        
        return $result;
    }

    public function getObject($bucket, $key)
    {
        // Get an object.
        $result = $this->client->getObject(array(
            'Bucket' => $bucket,
            'Key'    => $key
        ));

        return $result;
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
            'StorageClass' => 'STANDARD'
        ));

        return $result;
    }

    public function pollObject($bucket, $key)
    {
        // We can poll the object until it is accessible
        $result = $this->client->waitUntil('ObjectExists', array(
            'Bucket' => $bucket,
            'Key'    => $key
        ));

        return $result;
    }

    public function deleteObject($bucket, $key)
    {
        $result = $this->client->deleteObject(array(
            'Bucket' => $bucket,
            'Key'    => $key
        ));

        return $result;
    }

    public function truncateBucket($bucket)
    {
        $iterator = $this->client->getIterator('ListObjects', array(
            'Bucket' => $bucket,
            'Prefix' => ""
        ));

        foreach ($iterator as $object) {
            $completeS3path = "s3://".$bucket."/".$object['Key'];

            echo $completeS3path."\n";

            $this->client->registerStreamWrapper();

            echo $object['Key']."\n";

            $keyExists = file_exists("s3://$bucket/".$object['Key']);

            if ($keyExists) {
                // Get an object using the getObject operation
                $content = $this->client->getObject(array(
                    'Bucket' => $bucket,
                    'Key'    => $object['Key']
                ));
            }

            $res = $this->deleteObject($bucket, $object['Key']);

            if ($res) {
                // print_r($res);
            }
        }
    }

}
