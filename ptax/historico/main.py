import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta

# Configurações iniciais
data_inicial = "2024-04-24"                         # Data inicial no formato YYYY-MM-DD
data_final = "2025-04-24"                           # Data final no formato YYYY-MM-DD
name_file = f"{data_inicial}--{data_final}.csv"     # Criando o nome do arquivo dinamicamente
intervalo_requisicoes = 5                           # Tempo de pausa em segundos após cada X requisições
limite_requisicoes = 10    

# URL e parâmetros fixos
url = "https://ptax.bcb.gov.br/ptax_internet/consultaBoletim.do?method=consultarBoletim"
parametros_fixos = {
    "RadOpcao": "3",
    "DATAFIM": "",
    "ChkMoeda": "61"
}

# Inicializar variáveis
dados = []
contador_requisicoes = 0

# Função para extrair dados de uma página
def extrair_dados(html, data):
    soup = BeautifulSoup(html, "html.parser")
    tabela = soup.find("table", {"class": "tabela"})
    if not tabela:
        return []  # Retorna vazio se não houver tabela
    linhas = tabela.find("tbody").find_all("tr")
    resultado = []
    for linha in linhas:
        colunas = linha.find_all("td")
        if len(colunas) >= 6:
            resultado.append({
                "Data": data,
                "Hora": colunas[0].text.strip(),
                "Tipo": colunas[1].text.strip(),
                "Compra": colunas[2].text.strip(),
                "Venda": colunas[3].text.strip(),
                "Paridade Compra": colunas[4].text.strip(),
                "Paridade Venda": colunas[5].text.strip()
            })
    return resultado

# Loop para percorrer as datas
data_atual = datetime.strptime(data_inicial, "%Y-%m-%d")
data_fim = datetime.strptime(data_final, "%Y-%m-%d")

while data_atual <= data_fim:
    parametros = parametros_fixos.copy()
    parametros["DATAINI"] = data_atual.strftime("%d/%m/%Y")

    try:
        resposta = requests.post(url, data=parametros)
        if resposta.status_code == 200:
            # Extrair dados e adicionar à lista
            dados_extracao = extrair_dados(resposta.text, data_atual.strftime("%Y-%m-%d"))
            dados.extend(dados_extracao)
        else:
            print(f"Falha ao consultar para a data {data_atual.strftime('%Y-%m-%d')} - Código: {resposta.status_code}")
    except Exception as e:
        print(f"Erro na requisição para a data {data_atual.strftime('%Y-%m-%d')}: {e}")

    # Controle de intervalo e contagem de requisições
    contador_requisicoes += 1
    if contador_requisicoes >= limite_requisicoes:
        print(f"Esperando {intervalo_requisicoes} segundos para evitar bloqueios...")
        time.sleep(intervalo_requisicoes)
        contador_requisicoes = 0

    # Próxima data
    data_atual += timedelta(days=1)

# Salvar os dados em CSV
df = pd.DataFrame(dados)
df.to_csv(name_file, index=False, sep=";")

print("Processo concluído. Dados salvos em 'dados_ptax.csv'.")