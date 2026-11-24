<?php
// Consumidor de telemetria via STOMP para o ActiveMQ.

require_once __DIR__ . '/config.php';

// Processamento de argumentos
$options = getopt('', [
    'id::',
    'mode::',
    'destination::',
    'timeout::',
    'help',
]);

if (isset($options['help'])) {
    echo "Uso: php consumer.php [opções]\n";
    echo "Opções:\n";
    echo "  --id=<valor>           Identificador desta instância consumidora (padrão: PID do processo)\n";
    echo "  --mode=<queue|topic>   Modo: 'queue' (padrão) ou 'topic'\n";
    echo "  --destination=<nome>   Fila ou tópico para assinar\n";
    echo "  --timeout=<segundos>   Tempo limite de espera pela mensagem (padrão: 120)\n";
    exit(0);
}

$instanceId = $options['id'] ?? (string)getmypid();
$mode = isset($options['mode']) ? strtolower($options['mode']) : 'queue';
$timeout = isset($options['timeout']) ? (int)$options['timeout'] : 120;

$destination = $options['destination'] ?? ($mode === 'topic' ? Config::getDefaultTopic() : Config::getDefaultQueue());

// Garante o prefixo /queue/ ou /topic/
if ($mode === 'topic' && !str_starts_with($destination, '/topic/')) {
    $destination = '/topic/' . ltrim($destination, '/');
} elseif ($mode === 'queue' && !str_starts_with($destination, '/queue/')) {
    $destination = '/queue/' . ltrim($destination, '/');
}

echo sprintf("[%s] [Instância #%s] Conectando ao ActiveMQ (%s:%d)...\n",
    date('H:i:s'), $instanceId, Config::getActiveMqHost(), Config::getActiveMqPort()
);

try {
    $stomp = Config::getStompClient($timeout);
} catch (\Exception $e) {
    echo sprintf("[%s] [Instância #%s] ERRO na conexão: %s\n", date('H:i:s'), $instanceId, $e->getMessage());
    exit(1);
}

// Inscrição com confirmação manual (ack: client)
// O ActiveMQ só removerá a mensagem da fila após receber o ACK explícito.
// Se a instância cair antes do ACK, a mensagem é devolvida para a fila.
$subHeader = [
    'ack' => 'client',
    'activemq.prefetchSize' => '1', // Garante que ActiveMQ entrega 1 mensagem por vez (fundamental para 5 instâncias)
];

echo sprintf("[%s] [Instância #%s] Inscrito em %s (ack: client, prefetch: 1). Aguardando mensagem (timeout: %ds)...\n",
    date('H:i:s'), $instanceId, $destination, $timeout
);

$subId = $stomp->subscribe($destination, null, 'client', $subHeader);

$startTime = time();
$messageReceived = false;

while ((time() - $startTime) < $timeout) {
    $frame = $stomp->read();

    if ($frame) {
        $messageReceived = true;
        $headers = $frame->getHeaders();
        $body = $frame->getBody();

        echo "\n---------------------------------------------------------------\n";
        echo sprintf(" [Instância #%s] ✔ MENSAGEM RECEBIDA COM SUCESSO!\n", $instanceId);
        echo "---------------------------------------------------------------\n";
        echo " Destino:       " . ($headers['destination'] ?? $destination) . "\n";
        echo " Message-ID:    " . ($headers['message-id'] ?? 'N/A') . "\n";
        echo " Persistente:   " . ($headers['persistent'] ?? 'false') . "\n";
        echo " Prioridade:    " . ($headers['priority'] ?? 'padrão') . "\n";
        echo " Timestamp MS:  " . ($headers['timestamp'] ?? 'N/A') . "\n";
        if (isset($headers['expires']) && $headers['expires'] !== '0') {
            echo " Expira em:     " . date('Y-m-d H:i:s', (int)($headers['expires'] / 1000)) . "\n";
        }
        echo " Headers extras: " . json_encode(array_filter($headers, fn($k) => !in_array($k, ['destination', 'message-id', 'persistent', 'priority', 'timestamp', 'expires', 'subscription']), ARRAY_FILTER_USE_KEY)) . "\n";
        echo "\n Corpo da Mensagem:\n";

        // Tenta formatar bonito se for JSON
        $decoded = json_decode($body, true);
        if ($decoded !== null) {
            echo json_encode($decoded, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE) . "\n";
        } else {
            echo $body . "\n";
        }

        // Envia confirmação (ACK) para o ActiveMQ
        $stomp->ack($frame);
        echo sprintf("\n [Instância #%s] ✔ ACK enviado ao ActiveMQ. Mensagem confirmada!\n", $instanceId);
        echo sprintf(" [Instância #%s] Concluiu sua leitura única. Finalizando processo.\n", $instanceId);
        echo "---------------------------------------------------------------\n";

        // Cancela inscrição e sai
        try {
            $stomp->unsubscribe($subId);
        } catch (\Exception $e) {
            // Ignora se conexão já estiver fechando
        }
        exit(0);
    }

    usleep(200000); // 200ms
}

echo sprintf("[%s] [Instância #%s] ⚠️ Timeout de %d segundos atingido sem mensagens.\n",
    date('H:i:s'), $instanceId, $timeout
);
exit(2);
