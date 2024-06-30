import yt_dlp
import os
import pandas as pd
import concurrent.futures
import time
import re
import configparser
from tqdm import tqdm
import nltk
from openpyxl import load_workbook

nltk.download('punkt')

def load_config(download_path, config_path=None):
    config = configparser.ConfigParser()
    if config_path is None:
        config_path = os.path.join(download_path, 'config.ini')
    
    if os.path.exists(config_path):
        config.read(config_path)
    else:
        config['settings'] = {
            'base_download_path': download_path,
            'ffmpeg_location': r'C:\\Users\\danil\\Downloads\\Download Python\\ffmpeg\\bin\\ffmpeg.exe'
        }
        with open(config_path, 'w') as configfile:
            config.write(configfile)
    
    return config

def get_video_info(url, ffmpeg_location):
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'format': 'bestvideo+bestaudio/best',
        'ffmpeg_location': ffmpeg_location,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
    return info

def get_best_format_id(formats):
    sorted_formats = sorted(
        (f for f in formats if 'height' in f and f['vcodec'] != 'none'),
        key=lambda x: int(x['height']),
        reverse=True
    )
    for f in sorted_formats:
        if f['height'] <= 720:
            return f['format_id']
    return None

def clean_title(title, index):
    title = re.sub(r'[^\w\s-]', '', title)
    title = re.sub(r'\s+', ' ', title).strip()
    
    title = re.sub(r'\bAula \d+\b', '', title).strip()
    cleaned_title = f"Aula {index:02d} - {title}"

    return cleaned_title

def download_video(url, download_path, index, log, excel_positions, ffmpeg_location, retries=3):
    attempt = 0
    video_title = None
    position_in_excel = excel_positions[index - 1] if index - 1 < len(excel_positions) else None

    while attempt < retries:
        try:
            info = get_video_info(url, ffmpeg_location)
            video_title = clean_title(info.get('title', 'Video'), index)
            output_name = f'{video_title}.%(ext)s'

            formats = info.get('formats', [])
            chosen_format_id = get_best_format_id(formats)
            
            if not chosen_format_id:
                log.append((f"Aula {index:02d} - {video_title}", "Nenhuma resolução adequada disponível", position_in_excel))
                return False

            ydl_opts = {
                'format': f'{chosen_format_id}+bestaudio/best',
                'outtmpl': os.path.join(download_path, output_name),
                'merge_output_format': 'mp4',
                'ffmpeg_location': ffmpeg_location,
                'concurrent_fragment_downloads': 5,
                'ratelimit': None
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            log.append((f"Aula {index:02d} - {video_title}", f"Tentativa {attempt + 1} concluída", position_in_excel))
            return True
        except yt_dlp.DownloadError as e:
            attempt += 1
            log.append((f"Aula {index:02d} - {video_title}", f"Tentativa {attempt} falhou: {e}", position_in_excel))
            time.sleep(2)
        except Exception as e:
            log.append((f"Aula {index:02d} - {video_title}", f"Erro não esperado: {e}", position_in_excel))
            return False
    log.append((f"Aula {index:02d} - {video_title}", f"Erro após {retries} tentativas", position_in_excel))
    return False

def extract_number_from_string(s):
    match = re.search(r'\d+', s)
    return int(match.group()) if match else float('inf')

def process_planilha(planilha_path, base_download_path, max_threads, ffmpeg_location):
    modulo_nome = os.path.splitext(os.path.basename(planilha_path))[0]
    download_path = os.path.join(base_download_path, modulo_nome)

    if not os.path.exists(download_path):
        os.makedirs(download_path)

    try:
        df = pd.read_excel(planilha_path)
    except Exception as e:
        print(f"Erro ao ler a planilha {planilha_path}: {e}")
        return

    urls = df['URLs']
    if urls.empty:
        print(f"Nenhum link encontrado na planilha {planilha_path}.")
        return

    excel_positions = ['A' + str(i + 2) for i in df.index.tolist()]
    log = []
    total_count = len(urls)

    sequential_download = False
    with tqdm(total=total_count, desc="Baixando vídeos") as pbar:
        for index, url in enumerate(urls, start=1):
            if sequential_download:
                success = download_video(url, download_path, index, log, excel_positions, ffmpeg_location)
                if not success:
                    print(f"Erro ao baixar o vídeo {index}. Tentativas esgotadas.")
                    break
                else:
                    sequential_download = False
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
                    futures = [
                        executor.submit(download_video, url, download_path, index, log, excel_positions, ffmpeg_location)
                    ]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            success = future.result()
                            if not success:
                                sequential_download = True
                                break
                        except Exception as e:
                            print(f"Erro ao processar vídeo: {e}")
            pbar.update(1)

    success_count = sum(1 for entry in log if "concluída" in entry[1])
    failed_videos = [(entry[0], entry[2]) for entry in log if "concluída" not in entry[1]]

    unique_failed_videos = list(set(failed_videos))
    unique_failed_videos.sort(key=lambda x: extract_number_from_string(x[0]))

    log_filename = 'Download Concluido.txt' if not unique_failed_videos else 'Aulas Faltando.txt'
    log_file_path = os.path.join(download_path, log_filename)

    with open(log_file_path, 'w', encoding='utf-8') as log_file:
        log_file.write(f"Módulo: {modulo_nome}\n")
        log_file.write(f"Diretório de Download: {download_path}\n\n")
        
        log_file.write("Downloads concluídos:\n")
        for entry in log:
            if "concluída" in entry[1]:
                attempt = entry[1].split(" ")[1]
                log_file.write(f"  - {entry[0].split(' - ', 1)[0]} - {entry[0].split(' - ', 1)[1]} : Tentativa {attempt}\n")
        log_file.write("\n-------------------------------------------------------------------------------------------------\n")
        
        num_aulas_nao_baixadas = len(unique_failed_videos)
        log_file.write(f"Aulas não baixadas: {num_aulas_nao_baixadas} de {total_count}\n")
        for video, position_in_excel in unique_failed_videos:
            if position_in_excel is not None:
                log_file.write(f"  - {video}: Posição - {position_in_excel}\n")
        log_file.write("\n-------------------------------------------------------------------------------------------------\n")
        
        log_file.write(f"Total de vídeos baixados com sucesso: {success_count} de {total_count}\n")
        if success_count == total_count:
            log_file.write("Download concluído com sucesso.\n")
        else:
            log_file.write("Houve erros durante o download. Verifique as mensagens acima para mais detalhes.\n")

        log_file.write(f"\nCaminho da Planilha: {planilha_path}\n")

    if success_count == total_count:
        print(f"Todos os vídeos da planilha {planilha_path} foram baixados com sucesso!")
    else:
        print(f"O processo de download da planilha {planilha_path} terminou com alguns erros. Verifique o log para mais detalhes.")

def get_url_from_excel_cell(planilha_caminho, cell_reference):
    try:
        workbook = load_workbook(filename=planilha_caminho, data_only=True)
        sheet = workbook.active
        cell_value = sheet[cell_reference].value
        workbook.close()
        return cell_value
    except Exception as e:
        print(f"Erro ao acessar a célula {cell_reference} da planilha {planilha_caminho}: {e}")
        return None

def verify_modules(base_download_path, ffmpeg_location):
    logs_to_verify = []
    
    for root, dirs, files in os.walk(base_download_path):
        for file in files:
            if file == 'Aulas Faltando.txt':
                logs_to_verify.append(os.path.join(root, file))

    for log_path in logs_to_verify:
        module_name = os.path.basename(os.path.dirname(log_path))
        with open(log_path, 'r', encoding='utf-8') as log_file:
            lines = log_file.readlines()

        # Extrair informações do log
        missing_videos = []
        planilha_caminho = None
        for line in lines:
            if line.startswith("  - Aula") and "Posição - " in line:
                parts = line.strip().split(':')
                if len(parts) == 2:
                    video_title = parts[0].strip()
                    cell_reference = parts[1].strip().split(' - ')[-1]
                    missing_videos.append((video_title, cell_reference))
            if line.startswith("Caminho da Planilha:"):
                planilha_caminho = line.split(':', 1)[1].strip()

        if not planilha_caminho or not os.path.exists(planilha_caminho):
            print(f"Planilha para o módulo {module_name} não encontrada.")
            continue

        verification_log = []
        all_verified = True

        for video_title, cell_reference in missing_videos:
            url = get_url_from_excel_cell(planilha_caminho, cell_reference)
            if not url:
                print(f"URL não encontrado na célula {cell_reference} da planilha {planilha_caminho}")
                verification_log.append((video_title, "OFF"))
                all_verified = False
                continue

            # Extrair o índice do título do vídeo
            match = re.search(r'Aula (\d+)', video_title)
            if match:
                index = int(match.group(1))
            else:
                index = 1  # Se não encontrar o índice, use 1 como padrão

            success = download_video(url, os.path.dirname(log_path), index, verification_log, [cell_reference], ffmpeg_location)
            verification_log.append((video_title, "OK" if success else "OFF"))
            if not success:
                all_verified = False

        # Escrever o log de verificação
        verification_log_path = os.path.join(os.path.dirname(log_path), 'Verificacao.txt')
        with open(verification_log_path, 'w', encoding='utf-8') as v_log_file:
            v_log_file.write(f"Módulo: {module_name}\n")
            v_log_file.write(f"Diretório de Download: {os.path.dirname(log_path)}\n\n")
            
            v_log_file.write("Aulas não baixadas anteriormente:\n")
            for video_title, cell_reference in missing_videos:
                v_log_file.write(f"  - {video_title}\n")

            v_log_file.write("\nVerificação:\n")
            for video_title, status in verification_log:
                v_log_file.write(f"  - {video_title}: {status}\n")

            v_log_file.write(f"\nAulas verificadas com sucesso: {len([v for v, s in verification_log if s == 'OK'])} de {len(missing_videos)}\n")

        # Atualizar o nome do log de faltas se todas as aulas foram baixadas com sucesso
        if all_verified:
            os.rename(log_path, log_path.replace('Aulas Faltando.txt', 'Download Concluido.txt'))

def main():
    operacao = input("Digite 'download' para efetuar downloads ou 'verificacao' para verificar aulas faltantes: ").strip().lower()
    
    if operacao == 'download':
        diretorio_planilhas = input("Digite o diretório onde estão as planilhas com os links das aulas: ")
        base_download_path = input("Digite o diretório onde os downloads serão salvos: ")

        config = load_config(base_download_path)
        ffmpeg_location = config.get('settings', 'ffmpeg_location')

        planilhas = sorted(
            [os.path.join(diretorio_planilhas, f) for f in os.listdir(diretorio_planilhas) if f.endswith('.xlsx')],
            key=extract_number_from_string
        )

        print("\nPlanilhas disponíveis para download:")
        for i, planilha in enumerate(planilhas, 1):
            print(f"{i}: {os.path.basename(planilha)}")

        print("\nDigite os números das planilhas que você deseja selecionar, separados por espaço.")
        print("Digite 'tudo' para selecionar todas as planilhas.")
        selecao = input("Sua escolha: ")

        if selecao.strip().lower() == 'tudo':
            planilhas_selecionadas = planilhas
        else:
            indices = map(int, selecao.split())
            planilhas_selecionadas = [planilhas[i - 1] for i in indices]

        max_threads = min(8, os.cpu_count() + 4)

        for planilha_path in planilhas_selecionadas:
            print(f"Processando planilha: {planilha_path}")
            process_planilha(planilha_path, base_download_path, max_threads, ffmpeg_location)

    elif operacao == 'verificacao':
        base_download_path = input("Digite o diretório onde os módulos a serem verificados estão: ")

        # Carregar a configuração para obter a localização do ffmpeg
        config = load_config(base_download_path)
        ffmpeg_location = config.get('settings', 'ffmpeg_location')

        verify_modules(base_download_path, ffmpeg_location)

if __name__ == "__main__":
    main()
