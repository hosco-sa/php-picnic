<?php

namespace Picnic;

class SimpleUtils {

    /**
     * @return string
     */
    public static function new_uuid()
    {
        return strtolower(sprintf('%04X%04X-%04X-%04X-%04X-%04X%04X%04X', mt_rand(0,65535), mt_rand(0,65535), mt_rand(0,65535), mt_rand(16384,20479), mt_rand(32768,49151), mt_rand(0,65535), mt_rand(0,65535), mt_rand(0,65535)));
    }

    /**
     * @param $md5
     * @return string
     */
    public static function md5_to_uuid($md5)
    {
        return strtolower(substr($md5,0,8)."-".substr($md5,8,4)."-".substr($md5,12,4)."-".substr($md5,16,4)."-".substr($md5,20,12));
    }

    /**
     * @param $url
     * @return bool|int
     */
    public static function url_exists($url)
    {
        $hdrs = @get_headers($url);
        return is_array($hdrs) ? preg_match('/^HTTP\\/\\d+\\.\\d+\\s+2\\d\\d\\s+.*$/',$hdrs[0]) : false;
    }

    /**
     * @param $image
     * @return bool
     */
    public static function check_image($image) {
        //checks if the file is a browser compatible image

        $mimes = array('image/gif','image/jpeg','image/pjpeg','image/png');
        //get mime type
        $mime = getimagesize($image);
        $mime = $mime['mime'];

        $extensions = array('jpg','png','gif','jpeg');
        $extension = strtolower( pathinfo( $image, PATHINFO_EXTENSION ) );

        if ( in_array( $extension , $extensions ) AND in_array( $mime, $mimes ) ) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * @param $string
     * @return bool
     */
    public static function is_json($string) {
        json_decode($string);
        return (json_last_error() == JSON_ERROR_NONE);
    }

    /**
     * @param $num
     * @return string
     */
    public static function random_words($num)
    {
        $dictionary = "/usr/share/dict/words";

        $arWords = file($dictionary);

        $n = 0;

        $arRet = array();

        while ($n < $num) {
            $random = rand(0,235885);
            $arRet[] = rtrim($arWords[$random]);

            $n++;
        }

        return implode(" ", $arRet);
    }

    /**
     * @param array $array
     * @param $column
     * @param array $values
     * @param bool $notIn
     * @return array
     */
    public static function array_filter_values(array $array, $column, array $values, $notIn = false)
    {
        sort($values);

        return array_filter($array, function($item) use ($column, $values, $notIn) {
            return ($notIn xor (Utils::binary_search($item[$column], $values) !== false));
        });
    }

    /**
     * @param array $array
     * @param callable $callback
     * @param array $keys
     */
    public static function call_func_array(array &$array, callable $callback, array $keys = array())
    {
        $auxArray = empty($keys) ? $array : array_intersect_key($array, array_flip($keys));

        foreach ($auxArray as $key => $value) {
            $array[$key] = call_user_func($callback, $value);
        }
    }

    /**
     * @param array $array
     * @param $field
     * @param bool $reverse
     */
    public static function usort_by_array_field(array &$array, $field, $reverse = false)
    {
        uasort($array, function($item1, $item2) use ($field, $reverse) {
            return (($reverse xor $item1[$field] < $item2[$field]) ? -1 : 1);
        });
    }

    /**
     * @param array $array
     * @param $column
     * @param array $values
     * @return array
     */
    public static function array_intersect_column(array $array, $column, array $values)
    {
        $colA = array_flip(array_column($array, $column));
        $colB = array_flip($values);

        $intersect = array_intersect_key($colA, $colB);

        return array_intersect_key($array, array_flip($intersect));
    }

    public static function curl_image($url, $file)
    {
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT, 5);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch,CURLOPT_USERAGENT,'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.13) Gecko/20080311 Firefox/2.0.0.13');
        curl_setopt($ch, CURLOPT_BINARYTRANSFER,1);
        $rawdata = curl_exec ($ch);
        curl_close ($ch);

        $fp = fopen($file,'w');
        fwrite($fp, $rawdata);
        fclose($fp);
    }

    public static function curl_request($url, $type="GET", $json=false)
    {
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT, 5);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_USERAGENT,'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.13) Gecko/20080311 Firefox/2.0.0.13');
        curl_setopt($ch, CURLOPT_BINARYTRANSFER,1);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $type);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $json);
        curl_setopt($ch, CURLOPT_HTTPHEADER, array('Accept: application/json'));
        $rawdata = curl_exec ($ch);
        curl_close ($ch);

        return $rawdata;
    }

    /**
     * @param $url
     * @return int
     */
    public static function up_check($url)
    {
        $up = 0;

        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT, 5);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_VERBOSE, 0);
        curl_setopt($ch, CURLOPT_HEADER, 1);
        $response = curl_exec($ch);

        if ($response) {
            $up = 1;
        }

        // Then, after your curl_exec call:
        $header_size = curl_getinfo($ch, CURLINFO_HEADER_SIZE);
        $header = substr($response, 0, $header_size);
        $body = substr($response, $header_size);

        return $up;
    }

    public static function pass_hash($password)
    {
        $hash = password_hash($password, PASSWORD_DEFAULT);
        // echo $hash."\n";
        
        return $hash;
    }

    public static function pass_check($password, $hash)
    {
        if (password_verify($password, $hash)) {
            // echo 'Password is valid!';
            return true;
        } else {
            // echo 'Invalid password.';
            return false;
        }
    }

    public static function get_server_headers()
    {
        $headers = '';
        foreach ($_SERVER as $name => $value)
        {
            if (substr($name, 0, 5) == 'HTTP_')
            {
                $headers[str_replace(' ', '-', ucwords(strtolower(str_replace('_', ' ', substr($name, 5)))))] = $value;
            }
        }
        return $headers;
    }
}