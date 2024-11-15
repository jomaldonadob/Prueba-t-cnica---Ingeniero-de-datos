#!/bin/bash

DIRECTORIO="/app/input"


echo "Monitoreando el directorio: $DIRECTORIO"

inotifywait -m -e create "$DIRECTORIO" |
while read -r ruta evento archivo; do
    ARCHIVO_COMPLETO_INPUT="${ruta}${archivo}"
    ARCHIVO_COMPLETO_OUTPUT="/app/output/${archivo}"
    echo "Se ha detectado la creación de un nuevo archivo: $ARCHIVO_COMPLETO_INPUT"

    python3 main.py "$ARCHIVO_COMPLETO_INPUT" "$ARCHIVO_COMPLETO_OUTPUT"

    echo "Se ha creado el output en: $ARCHIVO_COMPLETO_OUTPUT"
done