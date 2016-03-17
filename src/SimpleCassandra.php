<?php
namespace Picnic;

class SimpleCassandra
{
    public $client;

    public function __construct($cassandra)
    {
        $this->client = $cassandra;
    }

    public function deleteSingleKeyspace($keyspace)
    {
        $arKeySpaces = array();
        $arKeySpaces[] = $keyspace;

        foreach ($arKeySpaces as $keyspace) {
            // Drop keyspace
            $result = $this->client->query('DROP KEYSPACE IF EXISTS "'.$keyspace.'";');
        }

        return $result;
    }

    public function createSingleKeyspace($keyspace)
    {
        $arKeySpaces = array();
        $arKeySpaces[] = $keyspace;

        foreach ($arKeySpaces as $keyspace) {
            // Create keyspace
            $result = $this->client->query('CREATE KEYSPACE IF NOT EXISTS "'.$keyspace.'" WITH REPLICATION = {\'class\': \'SimpleStrategy\', \'replication_factor\': 2};');

            // Change keyspace at runtime
            $result = $this->client->setKeyspace($keyspace);

            // Drop and Create ads table
            $result = $this->client->query('DROP TABLE IF EXISTS ads;');
            $result = $this->client->query('CREATE TABLE IF NOT EXISTS ads (uuid uuid, client varchar, entity varchar, id varchar, migration varchar, action varchar, microtime varchar, microtime_rcv varchar, body text, subject text, user_id varchar, PRIMARY KEY (uuid));');

            // Drop and Create ads table
            $result = $this->client->query('DROP TABLE IF EXISTS users;');
            $result = $this->client->query('CREATE TABLE IF NOT EXISTS users (uuid uuid, client varchar, entity varchar, id varchar, migration varchar, action varchar, microtime varchar, microtime_rcv varchar, email text, name text, PRIMARY KEY (uuid));');
        }

        return;
    }

    public function createTablesCassandra()
    {
        $arKeySpaces = array();
        $arKeySpaces[] = "avito";
        $arKeySpaces[] = "corotos";
        $arKeySpaces[] = "smmx";
        $arKeySpaces[] = "tayara";
        $arKeySpaces[] = "tori";
        $arKeySpaces[] = "yapo";

        foreach ($arKeySpaces as $keyspace) {
            // Drop keyspace
            $result = $this->client->query('DROP KEYSPACE IF EXISTS "'.$keyspace.'";');

            // Create keyspace
            $result = $this->client->query('CREATE KEYSPACE IF NOT EXISTS "'.$keyspace.'" WITH REPLICATION = {\'class\': \'SimpleStrategy\', \'replication_factor\': 2};');

            // Change keyspace at runtime
            $result = $this->client->setKeyspace($keyspace);

            // Drop and Create ads table
            $result = $this->client->query('DROP TABLE IF EXISTS ads;');
            $result = $this->client->query('CREATE TABLE IF NOT EXISTS ads (uuid uuid, client varchar, entity varchar, id varchar, migration varchar, action varchar, microtime varchar, microtime_rcv varchar, body text, subject text, user_id varchar, PRIMARY KEY (uuid));');

            // Drop and Create ads table
            $result = $this->client->query('DROP TABLE IF EXISTS users;');
            $result = $this->client->query('CREATE TABLE IF NOT EXISTS users (uuid uuid, client varchar, entity varchar, id varchar, migration varchar, action varchar, microtime varchar, microtime_rcv varchar, email text, name text, PRIMARY KEY (uuid));');
        }

        return;
    }

    public function processRecord($record, $origin=false, $filter=false)
    {
        $data = json_decode($record['Data'], true);

        $client = $data['client'];

        $microtime = $data['microtime'];

        $microtimeRCV = $data['microtime-rcv'];

        $event = json_decode($data['event'], true);

        if (isset($event['object']['insert']['region']['region_name']))
            foreach ($event['object']['insert']['region']['region_name'] as $k => $v) {

                $v = Encoding::fixUTF8($v);

                $event['object']['insert']['region']['region_name_'.$k] = $v;
                unset($event['object']['insert']['region']['region_name'][$k]);
            }

        if (isset($event['object']['insert']['category']['category_name']))
            foreach ($event['object']['insert']['category']['category_name'] as $k => $v) {

                $v = Encoding::fixUTF8($v);

                $event['object']['insert']['category']['category_name_'.$k] = $v;
                unset($event['object']['insert']['category']['category_name'][$k]);
            }

        $id = '';
        $entity = '';

        if (isset($event['object']['ad_id'])) {
            $id = $event['object']['ad_id'];
            $entity = "ad";
        } else if (isset($event['object']['user_id'])) {
            $id = $event['object']['user_id'];
            $entity = "user";
        }

        $seed = "$client-$entity-$id";

        $md5 = md5($seed);
        $md5UUID = SimpleFunctions::md5ToUUID($md5);

        $object = array();

        $object['uuid'] = $md5UUID;
        $object['client'] = $client;
        $object['entity'] = $entity;
        $object['id'] = $id;
        $object['migration'] = 'Y';
        $object['microtime'] = $microtime;
        $object['microtime_rcv'] = $microtimeRCV;

        if (isset($origin)) {
            $object['origin'] = $origin;
        }

        if (isset($event['object']['insert'])) {
            $object['action'] = 'insert';
            foreach ($event['object']['insert'] as $k => $v) {
                $object[$k] = $v;
            }
        } else if (isset($event['object']['update'])) {
            $object['action'] = 'update';
            foreach ($event['object']['insert'] as $k => $v) {
                $object[$k] = $v;
            }
        }

        $object['email'] = md5($object['email']);
        $object['name'] = md5($object['name']);
        $object['phone'] = md5($object['phone']);

        if (!strstr($client, "_debug")) {

            // Keyspace can be changed at runtime
            if (isset($filter) && $filter != '') {
                if ($client == $filter) {
                    $result = $this->client->setKeyspace($client);
                }
            } else {
                $result = $this->client->setKeyspace($client);
            }

            // Batch 1
            if ($object['entity'] == 'ad') {

                $object['body'] = rtrim($object['body']);
                $object['subject'] = rtrim($object['subject']);

                try {
                    foreach ($object as $key => $value) {
                        if ($key != 'uuid') {
                            if (isset($value) && !is_array($value) && $value != '') {
                                $object[$key] = "'".rtrim(str_replace("'", "''", $value))."'";
                            } else {
                                $object[$key] = "''";
                            }
                        }
                    }

                    $query = "INSERT INTO {$client}.ads (uuid, client, entity, id, migration, action, microtime, microtime_rcv, body, subject, user_id) VALUES ({$object['uuid']}, {$object['client']}, {$object['entity']}, {$object['id']}, {$object['migration']}, {$object['action']}, {$object['microtime']}, {$object['microtime_rcv']}, {$object['body']}, {$object['subject']}, {$object['user_id']});";
                    // echo $query."\n";

                    if (isset($filter) && $filter != '') {
                        if ($client == $filter) {
                            $res = $this->client->query($query);
                        }
                    } else {
                        $res = $this->client->query($query);
                    }

                    if (isset($res)) {
                        echo "cassandra {$client} {$object['uuid']}\n";
                    }

                } catch (\Cassandra\Exception $e) {
                    echo $e->getMessage();
                }

            } else if ($object['entity'] == 'user') {

                if (isset($object['name'])) {
                    $object['name'] = rtrim($object['name']);
                } else {
                    $object['name'] = '';
                }

                try {
                    foreach ($object as $key => $value) {
                        if ($key != 'uuid') {
                            if (isset($value) && !is_array($value) && $value != '') {
                                $object[$key] = "'".rtrim(str_replace("'", "''", $value))."'";
                            } else {
                                $object[$key] = "''";
                            }
                        }
                    }

                    $query = "INSERT INTO {$client}.users (uuid, client, entity, id, migration, action, microtime, microtime_rcv, email, name) VALUES ({$object['uuid']}, {$object['client']}, {$object['entity']}, {$object['id']}, {$object['migration']}, {$object['action']}, {$object['microtime']}, {$object['microtime_rcv']}, {$object['email']}, {$object['name']});";
                    // echo $query."\n";

                    if (isset($filter)) {
                        if ($client == $filter) {
                            $res = $this->client->query($query);
                        }
                    } else {
                        $res = $this->client->query($query);
                    }

                    if (isset($res)) {
                        echo "cassandra {$client} {$object['uuid']}\n";
                    }

                } catch (\Cassandra\Exception $e) {
                    echo $e->getMessage();
                }
            }
        }

        if (isset($result)) {
            return $result;
        }

    }

}

