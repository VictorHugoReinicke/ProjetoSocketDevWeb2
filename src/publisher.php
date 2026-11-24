<?php
// Publicador de telemetria via STOMP para o ActiveMQ.

require_once __DIR__ . '/config.php';

use Stomp\Transport\Message;

// Processamento de argumentos da linha de comando
$options = getopt('', [
    'interval::',
    'count::',
    'mode::',
    'destination::',
    'persistent::',
    'priority::',
    'ttl::',
    'help',
]);

if (isset($options['help'])) {
    echo "Uso: php publisher.php [opções]\n";
    echo "Opções:\n";
    echo "  --interval=<segundos>  Intervalo entre mensagens em segundos (padrão: 60)\n";
    echo "  --count=<int>          Número de mensagens a publicar (padrão: 5)\n";
    echo "  --mode=<queue|topic>   Modo de envio: 'queue' (ponto-a-ponto) ou 'topic' (fan-out) (padrão: queue)\n";
    echo "  --destination=<nome>   Nome customizado da fila ou tópico\n";
    echo "  --persistent=<true|false> Gravação persistente no disco KahaDB (padrão: true)\n";
    echo "  --priority=<0-9>       Prioridade da mensagem (padrão: 4)\n";
    echo "  --ttl=<ms>             Tempo de vida / expiração em ms (ex: 30000 para 30s)\n";
    exit(0);
}

$interval = isset($options['interval']) ? (int)$options['interval'] : 60;
$count = isset($options['count']) ? (int)$options['count'] : 5;
$mode = isset($options['mode']) ? strtolower($options['mode']) : 'queue';
$isPersistent = !isset($options['persistent']) || in_array(strtolower($options['persistent']), ['true', '1', 'yes']);
$priority = isset($options['priority']) ? (int)$options['priority'] : 4;
$ttl = isset($options['ttl']) ? (int)$options['ttl'] : null;

$destination = $options['destination'] ?? ($mode === 'topic' ? Config::getDefaultTopic() : Config::getDefaultQueue());

// Garante o prefixo /queue/ ou /topic/ esperado pelo ActiveMQ
if ($mode === 'topic' && !str_starts_with($destination, '/topic/')) {
    $destination = '/topic/' . ltrim($destination, '/');
} elseif ($mode === 'queue' && !str_starts_with($destination, '/queue/')) {
    $destination = '/queue/' . ltrim($destination, '/');
}

echo "===============================================================\n";
echo "   ActiveMQ Publisher - Protocolo STOMP / Telemetria IoT       \n";
echo "   (Base: Experimento WPEIF 2020 - Wesley Bezerra)             \n";
echo "===============================================================\n";
echo " Broker:       " . Config::getActiveMqHost() . ":" . Config::getActiveMqPort() . "\n";
echo " Destino:      " . $destination . " (" . ($mode === 'topic' ? 'Tópico / Fan-Out' : 'Fila / Competing Consumers') . ")\n";
echo " Total msgs:   " . $count . "\n";
echo " Intervalo:    " . $interval . " segundo(s) " . ($interval === 60 ? "(1 minuto)" : "(customizado)") . "\n";
echo " Persistente:  " . ($isPersistent ? 'SIM (KahaDB disk store)' : 'NÃO (apenas RAM)') . "\n";
echo " Prioridade:   " . $priority . " (escala 0-9)\n";
if ($ttl) {
    echo " TTL:          " . $ttl . " ms\n";
}
echo "===============================================================\n\n";

try {
    $stomp = Config::getStompClient();
} catch (\Exception $e) {
    echo "[ERRO] Falha ao conectar ao ActiveMQ: " . $e->getMessage() . "\n";
    exit(1);
}

// Payloads baseados no dataset de piscicultura do WPEIF 2020
for ($i = 1; $i <= $count; $i++) {
    $timestamp = date('Y-m-d H:i:s');
    
    // Dados simulados do sensor (tanque de piscicultura)
    $payloadData = [
        'mensagem_id' => $i,
        'origem' => 'Sensor_IoT_Tanque01',
        'timestamp' => $timestamp,
        'dados_sensor' => [
            'temperatura_celsius' => round(24.0 + (mt_rand(0, 30) / 10), 1),       // 24.0 a 27.0 °C
            'oxigenio_dissolvido_mg_l' => round(5.5 + (mt_rand(0, 20) / 10), 1),  // 5.5 a 7.5 mg/L
            'ph_agua' => round(6.8 + (mt_rand(0, 10) / 10), 1),                   // 6.8 a 7.8
            'turbidez_ntu' => round(10.0 + (mt_rand(0, 50) / 10), 1),             // 10.0 a 15.0 NTU
        ],
        'status' => ($i === 3 ? 'ALERTA_OXIGENIO_BAIXO' : 'NORMAL'),
        'metadados' => [
            'protocolo' => 'STOMP / ActiveMQ',
            'experimento' => 'WPEIF 2020 IoT Fish Farm',
            'modo_envio' => $mode,
            'persistente' => $isPersistent,
        ]
    ];

    $body = json_encode($payloadData, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);

    // Configuração dos Headers do protocolo STOMP / ActiveMQ
    $headers = [
        'persistent' => $isPersistent ? 'true' : 'false',
        'priority' => (string)$priority,
        'content-type' => 'application/json',
        'sensor-tipo' => 'aquicultura',
        'seq-id' => (string)$i,
    ];

    if ($ttl !== null) {
        // No ActiveMQ, 'expires' é o timestamp epoch absoluto em ms
        $headers['expires'] = (string)((int)(microtime(true) * 1000) + $ttl);
    }

    $message = new Message($body, $headers);

    echo sprintf("[%s] Enviando mensagem %d de %d...\n", date('H:i:s'), $i, $count);
    
    $stomp->send($destination, $message);

    echo sprintf("  ✔ Mensagem %d enviada com sucesso para %s!\n", $i, $destination);
    echo sprintf("    Persistência: %s | Prioridade: %d\n", $headers['persistent'], $priority);
    echo sprintf("    Payload (amostra): Temp: %.1f°C | OD: %.1f mg/L | pH: %.1f | Status: %s\n",
        $payloadData['dados_sensor']['temperatura_celsius'],
        $payloadData['dados_sensor']['oxigenio_dissolvido_mg_l'],
        $payloadData['dados_sensor']['ph_agua'],
        $payloadData['status']
    );

    // Se não for a última mensagem, aguarda o intervalo configurado
    if ($i < $count) {
        echo sprintf("  ⏳ Aguardando %d segundo(s) antes do próximo envio...\n\n", $interval);
        
        // Exibe progresso a cada 10 segundos se o intervalo for longo (ex: 60s)
        if ($interval >= 20) {
            $remaining = $interval;
            while ($remaining > 0) {
                $step = min(10, $remaining);
                sleep($step);
                $remaining -= $step;
                if ($remaining > 0) {
                    echo sprintf("    ... faltam %d segundos\n", $remaining);
                }
            }
        } else {
            sleep($interval);
        }
    }
}

echo "\n===============================================================\n";
echo "   Publicação de todas as {$count} mensagens concluída!        \n";
echo "===============================================================\n";
