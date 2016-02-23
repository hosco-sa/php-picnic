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

}