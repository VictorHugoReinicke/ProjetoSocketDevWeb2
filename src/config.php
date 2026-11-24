<?php
// Configuração da conexão STOMP com o ActiveMQ.

require_once __DIR__ . '/vendor/autoload.php';

use Stomp\Client;
use Stomp\StatefulStomp;

class Config
{
    public static function getActiveMqHost(): string
    {
        return getenv('ACTIVEMQ_HOST') ?: (self::isRunningInDocker() ? 'activemq' : '127.0.0.1');
    }

    public static function getActiveMqPort(): int
    {
        return (int)(getenv('ACTIVEMQ_PORT') ?: 61613);
    }

    public static function getActiveMqUser(): string
    {
        return getenv('ACTIVEMQ_USER') ?: 'admin';
    }

    public static function getActiveMqPass(): string
    {
        return getenv('ACTIVEMQ_PASS') ?: 'admin';
    }

    public static function getDefaultQueue(): string
    {
        return '/queue/iot_telemetria';
    }

    public static function getDefaultTopic(): string
    {
        return '/topic/iot_fanout';
    }

    /**
     * Cria e retorna uma instância conectada e autenticada do StatefulStomp
     */
    public static function getStompClient(int $timeout = 10): StatefulStomp
    {
        $host = self::getActiveMqHost();
        $port = self::getActiveMqPort();
        $uri = sprintf('tcp://%s:%d', $host, $port);

        $client = new Client($uri);
        $client->getConnection()->setReadTimeout($timeout);
        $client->setLogin(self::getActiveMqUser(), self::getActiveMqPass());

        return new StatefulStomp($client);
    }

    /**
     * Detecta se o script está rodando dentro de um container Docker
     */
    public static function isRunningInDocker(): bool
    {
        return file_exists('/.dockerenv') || (file_exists('/proc/1/cgroup') && strpos(file_get_contents('/proc/1/cgroup'), 'docker') !== false);
    }
}
