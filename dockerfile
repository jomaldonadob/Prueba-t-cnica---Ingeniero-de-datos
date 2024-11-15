# Usar una imagen base de Python 3.10
FROM python:3.10-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    python3-tk \
    openjdk-17-jre-headless \
    procps \
    inotify-tools \
    && rm -rf /var/lib/apt/lists/*

# Establecer JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Establecer el directorio de trabajo
WORKDIR /app

RUN mkdir input && mkdir output

# Copiar los archivos de requisitos y el código al contenedor
COPY requirements.txt requirements.txt
COPY . .

RUN chmod +x main_script.sh

# Copiar el archivo de datos al contenedor
COPY data/Films_2.xlsx /app/data/Films_2.xlsx

# Crear y activar un entorno virtual
RUN python -m venv venv
ENV PATH="/app/venv/bin:$PATH"

# Instalar las dependencias de Python en el entorno virtual
RUN pip install --no-cache-dir -r requirements.txt

# Comando para ejecutar el script principal
CMD ["bash", "main_script.sh"]
