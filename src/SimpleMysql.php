<?php

namespace Picnic;

class SimpleMysql {

    public $link;

    public $hostname;
    public $username;
    public $password;
    public $database;

    public $table;

    public $charset;

    /**
     * @param object $link
     * @param array $options
     */
    public function __construct($link=false, $options=array())
    {
        foreach($options AS $key => $value) {
            $this->$key = $value;
        }

        if ($link) {
            $this->link = $link;

        } else if (!empty($options)) {
            if (isset($this->host)) $this->hostname = $this->host;
            if (isset($this->dbname)) $this->database = $this->dbname;

            //$this->link = @mysqli_connect($this->hostname, $this->username, $this->password, $this->database);
            $this->link = mysqli_init();
            mysqli_options($this->link, MYSQLI_OPT_CONNECT_TIMEOUT, 5);

            if (@mysqli_real_connect($this->link, $this->hostname, $this->username, $this->password, $this->database)) {
                $names = "SET NAMES '".$this->charset."';";
                mysqli_query($this->link, $names);

                $charset = "SET CHARSET '".$this->charset."';";
                mysqli_query($this->link, $charset);
            } else
                $this->link = false;
        }
    }

    /**
     * READ Status
     *
     */
    public function readStatus()
    {

    }

    /**
     * CONNECTED
     *
     */
    public function connected()
    {
        return $this->link ? true : false;
    }

    /**
     * CREATE Database
     *
     */
    public function createDatabase()
    {

    }

    /**
     * ESCAPE String
     *
     */
    public function escapeString($string)
    {
        return mysqli_real_escape_string($this->link, $string);
    }

    /**
     * READ ALL Databases
     *
     */
    public function readAllDatabases()
    {
        $sql = "SHOW DATABASES";
        return $this->query($sql);
    }

    /**
     * DROP Database
     *
     */
    public function dropDatabase()
    {

    }

    /**
     * GET Columns
     *
     */
    public function getColumns($table = null)
    {
        $table = $table ? $table : $this->table;

        $sql = "SHOW COLUMNS FROM `".$table."`";
        return $this->query($sql);
    }

    /**
     * GET LastInsertID
     *
     */
    public function getLastInsertID()
    {
        return mysqli_insert_id($this->link);
    }

    /**
     * GET NextInsertID
     *
     */
    public function getNextInsertID($table = null)
    {
        $table = $table ? $table : $this->table;

        // $sql = "SELECT ".$table."_id+1 as next_id FROM ".$this->database.".$table WHERE 1 ORDER BY 1 DESC LIMIT 0,1;";
        $sql = "SELECT Auto_increment as next_id FROM information_schema.tables WHERE table_name='".$table."';";
        //$sql = "SELECT COUNT(*)+1 as next_id FROM `".$table."`;";

        $rows = $this->query($sql);
        return $rows[0]['next_id'];
    }

    /**
     * CREATE Row
     *
     */
    public function createRow($params, $replace = false, $table = null)
    {
        $table = $table ? $table : $this->table;

        $columns = $this->getColumns($table);

        foreach ($columns as $column) {
            $arColumns[] = $column['Field'];
        }

        // print_r($arColumns);

        if ($params)
            foreach ($params as $key => $value) {
                $value = is_array($value) ? json_encode($value) : $value;

                if (strstr($key, "eav_")) {
                    $eav[$key] = $value;
                } else if (!in_array($key, $arColumns)) {
                    $eav[$key] = $value;
                } else {
                    $keys[] = $key;
                    $values[] = mysqli_escape_string($this->link, $value);
                }
            }

        $sql = ($replace ? "REPLACE" : "INSERT") . " INTO ".$this->database.".".$table." (`".implode('`,`', $keys)."`) VALUES ('".implode("','", $values)."')";
        // echo $sql."\n";
        $this->query($sql);

        $lastId = $this->getLastInsertID();

        if (isset($eav) && $eav)
            foreach ($eav as $key => $value) {
                $sql =  "INSERT INTO ".$this->database.".".$table."_eav (`".$table."_id`, attribute, value) VALUES ($lastId, '$key', '$value')";
                // echo $sql."\n";
                $this->query($sql);
            }

        return $lastId;
    }

    /**
     * INSERT Replace Row
     *
     */
    public function insertReplaceRow($data, $table = null)
    {
        $table = $table ? $table : $this->table;

        $queryBuilder = "REPLACE INTO $table (%s) VALUES (%s)";

        $columns = join(",", array_keys($data));
        $values = join(",", array_fill(0, count($data), '?'));

        $query = sprintf($queryBuilder, $columns, $values);

        $types = str_pad('', count($data), 's');
        $params = array(&$types);
        foreach ($data as $field => $value) {
            $params[] =& $data[$field];
        }

        //mysqli_report(MYSQLI_REPORT_ALL);
        $stmt = $this->link->prepare($query);
        call_user_func_array(array($stmt, 'bind_param'), $params);

        $stmt->execute();

        return $this->getLastInsertID();
    }

    /**
     * READ Row
     *
     */
    public function readRow($id, $table = null)
    {
        $table = $table ? $table : $this->table;

        $sql = "SELECT * FROM ".$this->database.".".$table." WHERE 1 AND ".$table."_id = '" . $id . "'";
        return $this->query($sql);
    }

    /**
     * READ By Keys
     *
     */
    public function readByKeys($table, $keys)
    {
        $query = "SELECT * FROM $table WHERE 1";
        foreach ($keys as $field => $value) {
            $query .= " AND $field = '" . $this->escapeString($value) . "'";
        }

        return $this->query($query);
    }

    /**
     * COUNTS Rows
     *
     */
    public function countRows($priority=0, $table = null)
    {
        $table = $table ? $table : $this->table;

        if ($priority > 0) {
            $sql = "SELECT COUNT(*) as total_rows FROM ".$this->database.".".$table." WHERE 1 AND priority >= $priority";
        } else {
            $sql = "SELECT COUNT(*) as total_rows FROM ".$this->database.".".$table." WHERE 1";
        }

        $rows = $this->query($sql);
        return $rows[0]['total_rows'];
    }

    /**
     * COUNTS Rows From
     *
     */
    public function countRowsFrom($id, $keyColumn = null, $table = null)
    {
        $table = $table ? $table : $this->table;

        $keyColumn = $keyColumn ? $keyColumn : ($table . '_id');
        $sql = "SELECT COUNT(*) as total_rows FROM $table WHERE $keyColumn >= '{$id}'";

        $rows = $this->query($sql);

        return $rows[0]['total_rows'];
    }

    /**
     * COUNTS Active Rows
     *
     */
    public function countActiveRows($table = null)
    {
        $table = $table ? $table : $this->table;

        $sql = "SELECT COUNT(*) as total_rows FROM ".$table." WHERE 1 AND active='Y'";
        $rows = $this->query($sql);
        return $rows[0]['total_rows'];
    }

    /**
     * COUNTS Active Rows
     *
     */
    public function countActiveRowsFrom($id, $table = null)
    {
        $table = $table ? $table : $this->table;

        $sql = "SELECT COUNT(*) as total_rows FROM ".$this->database.".".$table." WHERE " . $table . "_id >= '" . $id  . "' AND active='Y'";
        $rows = $this->query($sql);
        return $rows[0]['total_rows'];
    }

    /**
     * COUNTS Non Active Rows
     *
     */
    public function countNonActiveRows($table = null)
    {
        $table = $table ? $table : $this->table;

        $sql = "SELECT COUNT(*) as total_rows FROM ".$this->database.".".$table." WHERE 1 AND active='N'";
        $rows = $this->query($sql);
        return $rows[0]['total_rows'];
    }

    /**
     * READ ALL Rows
     *
     */
    public function readAllRows($start=0, $size=30000000, $priority=0, $table = null)
    {
        $table = !$table ? $this->table : $table;

        if ($priority > 0) {
            $sql = "SELECT * FROM ".$this->database.".".$table." WHERE 1 AND priority >= $priority LIMIT $start, $size";
        } else {
            $sql = "SELECT * FROM ".$this->database.".".$table." WHERE 1 LIMIT $start, $size";
        }
        return $this->query($sql);
    }

    /**
     * READ ALL Rows From
     *
     */
    public function readAllRowsFrom($entityId = '0', $size = 30000000, $keyColumn = null, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $keyColumn = $keyColumn ? $keyColumn : $table . '_id';

        $sql = "SELECT * FROM " . $table . " WHERE $keyColumn > '$entityId' LIMIT $size";

        return $this->query($sql);
    }

    /**
     * READ ALL Active Rows
     * 
     */
    public function readAllActiveRows($start=0, $size=10000000, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $sql = "SELECT * FROM ".$table." WHERE 1 AND active='Y' LIMIT $start, $size";
        return $this->query($sql);
    }

    /**
     * READ Rows From
     *
     */
    public function readRowsFrom($entityId, $size = 10000, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $field = $table . '_id';

        $sql = "SELECT *
                    FROM %s
                    WHERE %s >=  '%s'
                    ORDER BY %s
                    LIMIT %d";
        $query = sprintf($sql, $table, $field, $entityId, $field, $size);

        return $this->query($query, true);
    }

    /**
     * READ All Active Rows From
     *
     */
    public function readAllActiveRowsFrom($entityId, $size = 10000, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $field = $table . '_id';

        $sql = "SELECT *
                    FROM %s
                    WHERE %s >=  '%s' AND active='Y'
                    ORDER BY %s
                    LIMIT %d";
        $query = sprintf($sql, $table, $field, $entityId, $field, $size);

        return $this->query($query);
    }

    /**
     * READ ALL Non Active Rows
     *
     */
    public function readAllNonActiveRows($start=0, $size=10000000, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $sql = "SELECT * FROM ".$this->database.".".$table." WHERE 1 AND active='N' LIMIT $start, $size";
        return $this->query($sql);
    }

    /**
     * READ Eav
     *
     */
    public function readEav($id, $filters=false, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $and = "";

        if ($filters) {
            foreach ($filters as $key => $value) {
                $and .= " $key = '$value'";
            }
        }

        $sql = "SELECT * FROM ".$table."_eav WHERE 1 AND ".$table."_id = '$id' $and";

        return $this->query($sql);
    }

    /**
     * READ All Eav
     *
     */
    public function readAllEav($id, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $sql = "SELECT * FROM ".$this->database.".".$table."_eav WHERE 1 AND ".$table."_id = $id";
        // echo $sql;
        return $this->query($sql);
    }

    /**
     * READ All Other
     *
     */
    public function readAllSecondary($id, $secondary, $interval = array(), $onlyActive = false, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $interval_where = '';
        if (!empty($interval)) {
            $interval_where = "AND {$interval['date_field']} > (CURRENT_DATE - INTERVAL {$interval['quantity']} {$interval['unit']})";
        }

        $active = '';
        if($onlyActive) {
            $active = "AND active = 'Y'";
        }

        $sql = "SELECT * FROM {$this->database}.{$table}_{$secondary} WHERE 1 AND {$table}_id = '$id' $active $interval_where ORDER BY 1 DESC";
        // echo $sql;
        return $this->query($sql);
    }

    /**
     * READ Secondary Data
     *
     */
    public function readSecondaryData($table, $keyColumn, $keyValue, $selectColumns = array(), $onlyActive = false, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $sqlSelect = join(',', $selectColumns);
        $sqlSelect = empty($sqlSelect) ? '*' : $sqlSelect;

        $sqlActive = $onlyActive ? "AND active = 'Y'" : '';
        $sql = "SELECT $sqlSelect FROM $table WHERE $keyColumn = '$keyValue' $sqlActive";

        return $this->query($sql);
    }

    /**
     * COUNTS Updated Rows
     *
     */
    public function countUpdatedRows($minute, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $sql = "SELECT COUNT(*) as total_rows FROM ".$this->database.".".$table." WHERE 1 AND updated_at > DATE_SUB( NOW() , INTERVAL $minute MINUTE )";
        $rows = $this->query($sql);
        if (isset($rows[0]['total_rows'])) {
            return $rows[0]['total_rows'];
        } else {
            return 0;
        }
    }

    /**
     * READ Updated Rows
     *
     */
    public function readUpdatedRows($minute, $start=false, $size=false, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $sql = "SELECT * FROM ".$this->database.".".$table." WHERE 1 AND updated_at > DATE_SUB( NOW() , INTERVAL $minute MINUTE ) LIMIT $start, $size";
        return $this->query($sql);
    }

    /**
     * UPDATE Row
     *
     */
    public function updateRow($id, $params, $table = null)
    {
        $table = !$table ? $this->table : $table;

        foreach ($params as $key => $value) {
            $sql = "UPDATE ".$this->database.".".$table." SET $key = '$value' WHERE ".$table."_id = $id";
            // echo $sql."\n";
            $this->query($sql);
        }
    }

    /**
     * DELETE Row
     *
     */
    public function deleteRow($id, $table = null)
    {
        $table = !$table ? $this->table : $table;

        $sql = "DELETE FROM ".$this->database.".".$table." WHERE ".$table."_id = $id LIMIT 1";
        // echo $sql."\n";
        return $this->query($sql);
    }

    /**
     * GET Next Auto Increment Id
     *
     */
    public function getNextAutoIncrementId($table = null)
    {
        $table = !$table ? $this->table : $table;

        $sql = "SELECT Auto_increment
                FROM information_schema.tables
                WHERE table_name = '".$table."'";
        $this->query($sql);
    }
    
    /**
     * QUERY
     *
     */
    public function query($sql, $firstColAsKey = false, $resultMode = MYSQLI_STORE_RESULT)
    {
        // echo $sql."\n";

        $res = mysqli_query($this->link, $sql, $resultMode);

        if (!$res) {
            return mysqli_error($this->link);
        }

        $arRows = array();

        if (!is_bool($res) && (strstr($sql, "SELECT ") || strstr($sql, "SHOW "))) {
            while ($row = mysqli_fetch_assoc($res)) {
                if ($firstColAsKey) {
                    $arRows[current($row)] = $row;
                } else {
                    $arRows[] = $row;
                }
            }

            return $arRows;
        } else {
            return $res;
        }
    }

}
