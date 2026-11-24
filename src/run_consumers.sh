#!/usr/bin/env bash
# Executa cinco consumidores concorrentes.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

DESTINATION="${1:-/queue/iot_telemetria}"
MODE="${2:-queue}"
TIMEOUT="${3:-120}"

echo "================================================================"
echo " Disparando 5 instâncias concorrentes do Consumer..."
echo " Destino: $DESTINATION | Modo: $MODE | Timeout: ${TIMEOUT}s"
echo "================================================================"

for i in {1..5}; do
    php consumer.php --id="$i" --destination="$DESTINATION" --mode="$MODE" --timeout="$TIMEOUT" &
    sleep 0.1
done

# Aguarda todas as 5 instâncias finalizarem
wait

echo ""
echo "================================================================"
echo " Todas as 5 instâncias concluíram suas leituras!"
echo "================================================================"
