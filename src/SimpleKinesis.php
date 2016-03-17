<?php

namespace Picnic;

class SimpleKinesis
{
    public $client;
    public $stream;

    public function __construct($aws, $stream=false)
    {
        $this->client = $aws->get('Kinesis');
        $this->stream = $stream;
    }

    public function getRecordsFromTrimedHorizon($stream, $limit)
    {
        echo "\nget shard iterator FromTrimedHorizon $stream ...\n";

        try {
            $result = $this->client->describeStream(array('StreamName' => $stream));
            $result = $result->toArray();
            // print_r($result);

            $result = $this->client->getShardIterator(array(
                // StreamName is required
                'StreamName' => $stream,
                // ShardId is required
                'ShardId' => '0',
                // ShardIteratorType is required
                'ShardIteratorType' => TRIM_HORIZON
            ));
            $result = $result->toArray();
            // print_r($result);

            $shardIterator = $result['ShardIterator'];

            $result = $this->client->getRecords(array(
                // ShardIterator is required
                'ShardIterator' => $shardIterator,
                'Limit' => $limit,
            ));
            $result = $result->toArray();
            // print_r($result);

            return $result;

        } catch (exception $e) {
            echo $e->getMessage();
        }


    }

    public function getRecordsFromSequenceNumber($stream, $limit, $ending_sequence_number)
    {
        echo "\nget shard iterator FromSequenceNumber...\n";

        $result = $this->client->getShardIterator(array(
            // StreamName is required
            'StreamName' => $stream,
            // ShardId is required
            'ShardId' => '0',
            // ShardIteratorType is required
            'ShardIteratorType' => AT_SEQUENCE_NUMBER,
            'StartingSequenceNumber' => $ending_sequence_number
        ));

        $result = $result->toArray();

        $shardIterator = $result['ShardIterator'];

        $result = $this->client->getRecords(array(
            // ShardIterator is required
            'ShardIterator' => $shardIterator,
            'Limit' => $limit,
        ));

        $result = $result->toArray();

        return $result;
    }


    public function getKinesisStreams($stream)
    {
        $result = $this->client->listStreams(array('Limit' => 10));

        $result = $result->toArray();

        foreach ($result['StreamNames'] as $s) {

            $arStreams[] = $s;

            // echo $s."\n";

            if ($s == $stream) {
                // echo "delete stream...\n";
                // $this->client->deleteStream(array( 'StreamName' => $stream ));
                // sleep(10);
            }
        }

        // echo "create stream...\n";
        // $this->client->createStream(array( 'StreamName' => $stream, 'ShardCount' => 1, ));
        // sleep(10);

        return $arStreams;
    }

    public function kinesisInsertTestRecords()
    {
        /*
        echo "put records...\n";
        $result = $this->client->putRecords(array(
            // Records is required
            'Records' => array(
                array(
                    // Data is required
                    'Data' => utf8_encode('{"data":"record1"}'),
                    // PartitionKey is required
                    'PartitionKey' => '1',
                    'ExplicitHashKey' => '1',
                    'SequenceNumberForOrdering' => 1,
                ),
                array(
                    // Data is required
                    'Data' => utf8_encode('{"data":"record2"}'),
                    // PartitionKey is required
                    'PartitionKey' => '1',
                    'ExplicitHashKey' => '1',
                    'SequenceNumberForOrdering' => 2,
                ),
                array(
                    // Data is required
                    'Data' => utf8_encode('{"data":"record3"}'),
                    // PartitionKey is required
                    'PartitionKey' => '1',
                    'ExplicitHashKey' => '1',
                    'SequenceNumberForOrdering' => 3,
                ),
            ),
            // StreamName is required
            'StreamName' => $stream,
        ));


        $result = $result->toArray();
        print_r($result);
        */
    }

}

