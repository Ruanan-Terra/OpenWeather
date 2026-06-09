FROM python:3.11-slim

WORKDIR /app

# Copia o requirements da raiz do Mac para dentro do container
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o script extract.py de dentro da pasta src do Mac para a raiz do container
COPY src/extract.py ./extract.py

# Define o comando padrão para executar o script quando o container iniciar
CMD ["python", "extract.py"]