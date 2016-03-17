<?php

namespace Picnic;

class SimpleCouchdb {

    public $user;
    public $pass;

    public $database;

    public $host;
    public $port;
    public $errno;
    public $errstr;

    public $request;
    public $response;
    public $body;

    /**
     * CONSTRUCT
     *
     */
    public function __construct($options)
    {
        foreach($options AS $key => $value) {
            $this->$key = $value;
        }
    }

    /**
     * READ Status
     *
     */
    public function readStatus()
    {
        return $this->send("GET", "/");
    }

    /**
     * CREATE Database
     *
     */
    public function createDatabase($database)
    {
        return $this->send("PUT", "/$database");
    }

    /**
     * DROP Database
     *
     */
    public function dropDatabase($database)
    {
        return $this->send("DELETE", "/$database");
    }

    /**
     * SET Database
     *
     */
    public function setDatabase($database)
    {
        $this->database = $database;
    }

    /**
     * READ ALL Databases
     *
     */
    public function readAllDatabases()
    {
        return $this->send("GET", "/_all_dbs");
    }

    /**
     * DELETE Database
     *
     */
    public function deleteDatabase()
    {
        return $this->send("DELETE", "/$this->database/");
    }

    /**
     * GET UUID
     *
     */
    public function getUUID()
    {
        $uuids = $this->send("GET", "/_uuids");

        $uuids = json_decode($uuids);

        return $uuids->uuids[0];
    }

    /**
     * COUNT Documents
     *
     */
    public function countDocuments($entity=false)
    {
        if (isset($entity)) {
            if (!strstr($this->database, $entity)) {
                $url = "/".$this->database."$entity";
            } else {
                $url = "/".$this->database;
            }
        } else {
            $url = "/".$this->database;
        }

        return $this->send("GET", $url);
    }

    /**
     * CREATE Document
     *
     */
    public function createDocument($params, $entity=false, $uuid=false, $includeZeroValues = array())
    {
        if (!$uuid) {
            $uuid = $this->getUUID();
        }

        $arParams = array();

        foreach ($params as $key => $value) {
            if (isset($value) && (($value !== '' && in_array($key, $includeZeroValues)) || ($value != '' && !in_array($key, $includeZeroValues)))) {
                if (!is_array($value)) {
                    $arParams[] = '"'.(rtrim($key)).'"'.':"'.(rtrim($value)).'"';
                } else {
                    $arParams[] = '"'.(rtrim($key)).'"'.':'.(json_encode($value));
                }
            }
        }

        $request = '{'.implode(", ", $arParams).'}';

        if (isset($entity)) {
            if (!strstr($this->database, $entity)) {
                $url = "/".$this->database."$entity/$uuid";
            } else {
                $url = "/".$this->database."/$uuid";
            }
        } else {
            $url = "/".$this->database."/$uuid";
        }

        // echo $url;

        return $this->send("PUT", $url, $request);
    }

    /**
     * READ Document
     *
     */
    public function readDocument($uuid, $entity=false)
    {
        if (isset($entity)) {
            if (!strstr($this->database, $entity)) {
                $url = "/".$this->database."$entity/$uuid";
            } else {
                $url = "/".$this->database."/$uuid";
            }
        } else {
            $url = "/".$this->database."/$uuid";
        }

        return $this->send("GET", $url);
    }

    /**
     * READ ALL Documents
     *
     */
    public function readAllDocuments($entity=false)
    {
        if (isset($entity)) {
            if (!strstr($this->database, $entity)) {
                $url = "/".$this->database."$entity/_all_docs";
            } else {
                $url = "/".$this->database."/_all_docs";
            }
        } else {
            $url = "/".$this->database."/_all_docs";
        }

        return $this->send("GET", $url);
    }

    public function readAllUsedDocuments($entity=false)
    {
        if (isset($entity)) {
            if (!strstr($this->database, $entity)) {
                $url = "/".$this->database."$entity/_all_docs";
            } else {
                $url = "/".$this->database."/_all_docs";
            }
        } else {
            $url = "/".$this->database."/_all_docs";
        }

        return $this->send("GET", $url);
    }
    /**
     * UPDATE Document
     *
     */
    public function updateDocument($uuid, $params_new, $entity, $check_empty = true)
    {
        $update = false;

        $document = $this->readDocument($uuid, $entity);
        $params_old = json_decode($document, true);

        if ($check_empty) {
            $arSkip = array("_id", "_rev", "id", "uuid");

            foreach ($params_old as $key => $value) {
                if (!isset($value) || $value == null || $value == '' || $value == 'null') {
                    unset($params_old[$key]);
                }
            }

            foreach ($params_new as $key => $value) {
                if (!isset($value) || $value == null || $value == '' || $value == 'null') {
                    unset($params_new[$key]);
                }
            }

            foreach ($params_new as $key => $value) {
                if (isset($params_old[$key]) && !in_array($key, $arSkip)) {
                    if ($params_old[$key] && $params_new[$key] && $params_old[$key] != $params_new[$key]) {
                        $params_old[$key] = $value;
                        $update = true;
                    }
                } else if (!in_array($key, $arSkip)) {
                    $params_old[$key] = $value;
                    $update = true;
                }
            }
        }
        else {
            $update = true;
        }

        $params = array_merge($params_old, $params_new);

        unset($params['error']);
        unset($params['reason']);

        $request = json_encode($params);

        if (isset($entity)) {
            if (!strstr($this->database, $entity)) {
                $url = "/".$this->database."$entity/$uuid";
            } else {
                $url = "/".$this->database."/$uuid";
            }
        } else {
            $url = "/".$this->database."/$uuid";
        }

        if ($update) {
            // echo "OLD: ".print_r($params_old)."\n";
            // echo "NEW: ".print_r($params_new)."\n";
            // echo "REQ: ".print_r($params)."\n";

            return $this->send("PUT", $url, $request);
        } else {
            return '{"updated":false}';
        }

    }

    /**
     * DELETE Document
     *
     */
    public function deleteDocument($uuid)
    {
        $doc = json_decode($this->readDocument($uuid), true);

        $url = "/$this->database/$uuid";
        if(!empty($doc['_rev'])) {
            $url .= "?rev={$doc['_rev']}";
            return $this->send("DELETE", $url);
        }

        return json_encode(array('updated' => false));
    }

    /**
     * DELETE All Documents
     *
     */
    public function deleteAllDocuments($entity)
    {
        $allDocs = $this->readAllDocuments($entity);

        $allDocs = json_decode($allDocs, true);

        foreach ($allDocs['rows'] as $doc) {
            $url = "/es_admin_cit_$entity/".$doc['id']."?rev=".$doc['value']['rev'];
            echo $url."\n";
            echo $this->send("DELETE", $url);
        }
    }

    /**
     * SEND
     *
     */
    private function send($method, $url, $post_data = NULL)
    {
        $url = rtrim($this->host, "/") . ':' . $this->port . $url;
        return $this->curlRequest($url, $post_data, $method);
    }

    /**
     * @param $api_url
     * @param $api_key
     * @param bool $body
     * @return mixed
     */
    private function curlRequest($url, $request=false, $type = 'POST')
    {
        $s = curl_init();

        curl_setopt($s, CURLOPT_CONNECTTIMEOUT, 1);
        curl_setopt($s, CURLOPT_TIMEOUT, 1);
        curl_setopt($s, CURLOPT_TIMEOUT, 5000);
        curl_setopt($s, CURLOPT_URL, $url);
        curl_setopt($s, CURLOPT_HTTPHEADER, array("Content-Type: application/json", "Accept: application/json"));
        curl_setopt($s, CURLOPT_CUSTOMREQUEST, $type);
        curl_setopt($s, CURLOPT_ENCODING , "");
        curl_setopt($s, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($s, CURLOPT_POSTFIELDS, $request);

        $start = microtime(true);

        $response = curl_exec($s);

        $finish = round((microtime(true) - $start)*1000)." ms";

        curl_close($s);

        $response = json_decode($response, true);

        $response['round_trip'] = $finish;

        $response = json_encode($response);

        return $response;
    }
}

?>
