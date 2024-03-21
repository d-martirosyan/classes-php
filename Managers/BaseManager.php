<?php

namespace LevCharity\Managers;


abstract class BaseManager
{
    public static $instances = [];

    public static function init()
    {
        new static();
    }

    public function __construct()
    {
        $this->afterConstruct();
        $this->addActions();
        $this->addFilters();
        $this->removeActions();
        $this->addShortCodes();
        $this->addOptions();
    }

    final public static function get_instance() {
        $class = get_called_class();

        if ( ! isset( $instances[ $class ] ) ) {
            self::$instances[ $class ] = new $class();
        }

        return self::$instances[ $class ];
    }

    protected function addActions()
    {
    }

    protected function addFilters()
    {
    }

    protected function addShortCodes()
    {
    }

    protected function removeActions()
    {
    }

    protected function addOptions()
    {
    }

    protected function afterConstruct()
    {
    }

    public function dump(...$vars)
    {
        echo "<pre>";
        var_dump(...$vars);
        echo "</pre>";
    }
}