import os
import traceback  
import requests
from common import secret_client, pubsub_client
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json
import unicodedata 

PROJECT_ID = os.environ.get("PROJECT_ID")

if PROJECT_ID == "dl-bq-prd":
    URL_BASE = "https://apiprod.prod.apimanagement.eu10.hana.ondemand.com/1/nossaexcelencia/api/"
else:
    ##URL_BASE = "https://agronex-dev.atvos.com/api/"
    URL_BASE = "https://apiprod.prod.apimanagement.eu10.hana.ondemand.com/1/nossaexcelencia/api/"

OPTION_LIST = {
    "goodday": "goodday/v1/goodday/",
    "macroindicator": "goodday/v1/macroindicator/",
    "gooddaytask": "goodday/v1/task/",
    "indleaderroute": "indleaderroute/v1/header/",
    "liderroute": "indleaderroute/v1/liderroute/",
    "indleaderroutevalue": "indleaderroute/v1/value/",
    "registrations": "registrations/v1/aspects/",
    "checklists": "registrations/v1/checklists/",
    "criticalities": "registrations/v1/criticalities/",
    "levelsubareas": "registrations/v1/levelsubareas/",
    "macroindicators": "registrations/v1/macroindicators/",
    "macroprocesses": "registrations/v1/macroprocesses/",
    "operationalkpis": "registrations/v1/operationalkpis/",
    "origensinfos": "registrations/v1/origensinfos/",
    "pillars": "registrations/v1/pillars/",
    "processes": "registrations/v1/processes/",
    "questions": "registrations/v1/questions/",
    "responsibles": "registrations/v1/responsibles/",
    "routes": "registrations/v1/routes/",
    "shifts": "registrations/v1/shifts/",
    "status": "registrations/v1/status/",
    "subareas": "registrations/v1/subareas/",
    "subroutes": "registrations/v1/subroutes/",
    "typenotes": "registrations/v1/typenotes/",
    "units": "registrations/v1/units/",
    "satisfaction": "satisfaction_survey/v1/satisfaction_survey/",
    "shiftchanges": "shiftchange/v1/shiftchanges/",
    "unconformities": "unconformities/v1/unconformities/",
    "users": "users/",
    "tasks_un": "unconformities/v1/tasks/",
    "value": "indleaderroute/v1/value/",
    "headerapproach": "approach/v1/headerapproach/",
    "valueapproach": "approach/v1/valueapproach/",
    "leadershipmetas": "approach/v1/leadershipmetas/",
    "member": "approach/v1/member/",
    #"note": "approach/v1/note/",
    "sector": "approach/v1/sector/",
    "situation": "approach/v1/situation/",
    "feature_flags": "feature_flags/v1/feature_flags/",
    "goodday_areas": "goodday/v1/goodday_areas/",
    "goodday_attendance": "goodday/v1/goodday_attendance/",
    "calendar_agr": "agrleader_route/v1/calendar_agr/",
    "equipment": "agrleader_route/v1/equipment/",
    "farm": "agrleader_route/v1/farm/",
    "header_agr": "agrleader_route/v1/header_agr/",
    "note_agr": "agrleader_route/v1/note_agr/",
    "question_agr": "agrleader_route/v1/question_agr/",
    "tasks_agr": "agrleader_route/v1/tasks_agr/",
    "unconformities_agr": "agrleader_route/v1/unconformities_agr/",
    "value_agr": "agrleader_route/v1/value_agr/",
    "value_equipment_agr": "agrleader_route/v1/value_equipment_agr/",
    "actionplan": "approach/v1/actionplan/",
    "administration": "approach/v1/administration/",
    "aspect": "approach/v1/aspect/"
}

TOPIC_NAME = "agronex_ingestion"
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")

def corrigir_acentos(text):
    # Normaliza a string para decompor caracteres acentuados
    text = unicodedata.normalize('NFD', text)
    # Remove apenas os sinais de acentuação (mantém caracteres especiais)
    text = ''.join([char for char in text if not unicodedata.combining(char)])
    return text

def corrigir_json(json_string):
    # Decodifica o JSON
    data = json.loads(json_string)
    
    # Corrige os acentos nos valores de string
    for key in data:
        if isinstance(data[key], str):
            data[key] = corrigir_acentos(data[key])
    
    # Retorna o JSON corrigido como uma string
    return json.dumps(data)

def prep(option):
    if option not in OPTION_LIST:
        raise ValueError(f"Opção inválida: {option}. Opções válidas são: {list(OPTION_LIST.keys())}")
    
    api_url = f'{URL_BASE}/{OPTION_LIST[option]}'
    
    username = secret_client.get_credential("carga_agronex_user")
    password = secret_client.get_credential("carga_agronex_pass")
    auth = HTTPBasicAuth(username, password)
    
    cliente_bigquery = bigquery.Client(project=PROJECT_ID)
    deletou = False
    publish_counter = 0
    total_paginas = 0  # Inicialização da variável
    total_itens = 0  # Inicialização da variável
    todos_os_resultados = []  # Lista para armazenar os resultados
    
    while api_url:
        try:
            response = requests.get(api_url, auth=auth, verify=True, timeout=90)
            response.raise_for_status()
        except Exception as e:
            print(f"Erro ao fazer requisição para {api_url}: {e}")
            traceback.print_exc()
            raise

        # Converte a resposta para JSON
        data = response.json()

        # Verifica se o retorno é uma lista
        if isinstance(data, list):
            print("Resposta é uma lista. Processando itens...")
            itens_por_pagina = len(data)
            total_paginas += 1
            total_itens += itens_por_pagina

            # Adicione os itens da lista diretamente ao resultado final
            todos_os_resultados.extend(data)
            print(f"Página {total_paginas} - Quantidade de itens: {itens_por_pagina}")

            # Como listas geralmente não têm paginação, finalize o loop
            break

        # Se o retorno é um dicionário
        elif isinstance(data, dict):
            print("Resposta é um dicionário. Processando...")
            results = data.get("results", [])
            itens_por_pagina = len(results)
            total_paginas += 1
            total_itens += itens_por_pagina

            # Adicione os resultados à lista principal
            todos_os_resultados.extend(results)
            print(f"Página {total_paginas} - Quantidade de itens: {itens_por_pagina}")

            # Atualize a URL para a próxima página
            api_url = data.get("next")
        else:
            # Se o tipo de resposta for inesperado
            print(f"Formato inesperado da resposta: {type(data)}")
            break

        if not deletou:
            delete_query = f"""
            DELETE FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE REGEXP_CONTAINS(data, r'rota/{option}:')
            """
            try:
                cliente_bigquery.query(delete_query).result()
                print("Registros antigos deletados com sucesso.")
            except NotFound:
                print("Nenhum registro encontrado para deletar com os critérios fornecidos.")
            except Exception as e:
                print(f"Erro ao tentar deletar registros: {e}")
                return {"status": "error", "message": "Erro ao deletar registros antigos", "details": str(e)}

            deletou = True

        for result in results:
            if isinstance(result, dict):
                result_str = json.dumps(result)
            else:
                result_str = str(result)

            # Corrige acentos no JSON
            result_str = corrigir_json(result_str)

            rota = "rota"
            message_str = f"{rota}/{option}: {result_str}"

            try:
                future = pubsub_client.publish(
                    pubsub_client.get_topic_path(TOPIC_NAME),
                    data=message_str.encode("utf-8"),
                    option=option
                )
                future.result()
                publish_counter += 1
            except Exception as e:
                print(f"Erro ao publicar dados no Pub/Sub: {e}")
                traceback.print_exc()
                continue

        api_url = data.get('next', None)
        if not api_url:
            print("Todas as páginas de dados foram processadas.")
            break

    print(f"Processamento concluído. Total de mensagens publicadas: {publish_counter}")
