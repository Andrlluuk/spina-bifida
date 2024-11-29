from PIL import Image
import torch
from stqdm import stqdm
from utils import *
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
import os
import uuid
import json

# Загрузка переменных окружения из .env файла
load_dotenv()


BUCKET = os.getenv('BUCKET')
result = {}
session_state = {}

###########
# Функции #
###########

# Функция чтения из S3
def get_from_yandex_cloud():
    ACCESS_KEY = os.getenv('ACCESS_KEY')
    SECRET_KEY = os.getenv('SECRET_KEY')
    ENDPOINT_URL = 'https://storage.yandexcloud.net'

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    result = []
    try:
        paginator = s3.get_paginator('list_objects')
        objects = s3.list_objects(Bucket=BUCKET)
        pages = paginator.paginate(Bucket=BUCKET)
        for page in pages:
            for obj in page['Contents']:
                result.append(obj['Key'])
    except FileNotFoundError:
        st.error("The file was not found.")
    except NoCredentialsError:
        st.error("An error with the credentials.")
    except Exception as e:
        st.error(f"An error has occurred: {e}")

    return result


# Функция записи на S3
def upload_to_yandex_cloud(file, file_name, bucket, object_name=None):
    ACCESS_KEY = os.getenv('ACCESS_KEY')
    SECRET_KEY = os.getenv('SECRET_KEY')
    ENDPOINT_URL = 'https://storage.yandexcloud.net'

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    try:
        s3.upload_file(file_name, bucket, object_name or file_name)
        os.remove(file_name) # Удаление локального файла после успешной загрузки
    except FileNotFoundError:
        st.error("The file was not found.")
    except NoCredentialsError:
        st.error("An error with the credentials.")
    except Exception as e:
        st.error(f"An error has occurred: {e}")

# Функция настройки моделей
def get_processor():
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    return MedicalImageProcessor(
        yolo_model_path='models/best_object_detection.pt',
        axial_quality_model_path='models/axial_quality.pt',
        axial_pathology_model_path='models/axial_pathology.pt',
        sagittal_quality_model_path='models/sagittal_quality.pt',
        sagittal_pathology_model_path='models/sagittal_pathology.pt',
        device=device
    )

# Функции обработки изображений
def cache_process_image(img_bytes, img_name, processor):
    return processor.process_image(img_bytes, img_name)

def process_uploaded_files(uploaded_files, processor):
    ACCESS_KEY = os.getenv('ACCESS_KEY')
    SECRET_KEY = os.getenv('SECRET_KEY')
    ENDPOINT_URL = 'https://storage.yandexcloud.net'

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    with stqdm(uploaded_files[:10], mininterval=1) as pbar:
        session_state['imgs'] = {}
        session_state['processed_images'] = {}
        for uploaded_file in pbar:
            try:
                get_object_response = s3.get_object(Bucket=BUCKET,Key=uploaded_file)
                img = Image.open(BytesIO(get_object_response['Body'].read()))
                img_bytes = io.BytesIO()
                img.save(img_bytes, format='PNG')
                img_bytes = img_bytes.getvalue()
                img_name = uploaded_file
                if img_name not in session_state['imgs']:
                    result = cache_process_image(img_bytes, img_name, processor)
                    session_state['imgs'][img_name] = img
                    session_state['processed_images'][img_name] = result
            except Exception as e:
                print(e)

# Функция генерации уникального идентификатора файла
def get_unique_id():
    unique_id = str(uuid.uuid4())
    return unique_id

def main():
    # Элемент для загрузки файлов
    uploaded_files = get_from_yandex_cloud()

    processor = get_processor()

    if uploaded_files:
        process_uploaded_files(uploaded_files, processor)

        processed_images = session_state['processed_images']
        imgs = session_state['imgs']

        files = list(processed_images.keys())
        count = 0
        for file in files:
            uniquie_id = get_unique_id()
            annotation_file_name = f'annotation_{uniquie_id}_{file}.json'
            img_file_name = f'img_{uniquie_id}_{file}'
            try:
                res = {
                    'img': img_file_name,
                    'old_file_name': file,
                    'quality': processed_images[file]["quality"]["prediction_prob"],
                    'pathology': processed_images[file]["pathology"]["prediction_prob"],
                    'roi': {
                            'prediction': processed_images[file]["plane"]['prediction_prob'].tolist(),
                            'plane': processed_images[file]["plane"]['plane'],
                            'box': {
                                'x1': processed_images[file]["plane"]['box'][0].tolist(),
                                'y1': processed_images[file]["plane"]['box'][1].tolist(),
                                'x2': processed_images[file]["plane"]['box'][2].tolist(),
                                'y2': processed_images[file]["plane"]['box'][3].tolist()
                                }
                            }
                    }
                count += 1
                # Временное хранение файлов
                session_state['imgs'][file].save(img_file_name)
                with open(annotation_file_name, 'w') as f:
                    json.dump(res, f)
            
                # Запись на S3
                upload_to_yandex_cloud(file, img_file_name, BUCKET, f'data/{img_file_name}')
                upload_to_yandex_cloud(file, annotation_file_name, BUCKET, f'annotation/{annotation_file_name}')

            except:
                res = {
                    'img': img_file_name,
                    'old_file_name': file,
                    'error': 'ROI not found'
                }

                # Временное хранение файлов
                session_state['imgs'][file].save(img_file_name)
                with open(annotation_file_name, 'w') as f:
                    json.dump(res, f)
                print(res)
                # Запись на S3
                upload_to_yandex_cloud(file, img_file_name, BUCKET, f'no_roi_data/{img_file_name}')
                upload_to_yandex_cloud(file, annotation_file_name, BUCKET, f'no_roi_annotation/{annotation_file_name}')
                


        # Вывод информации о загруженных и обработанных файлах
        col1, col2 = st.columns(2)
        with col1:
            st.metric(label='Uploaded', value=len(files))
        with col2:
            st.metric(label='ROI detected', value=count)
            
if __name__ == "__main__":
    main()
