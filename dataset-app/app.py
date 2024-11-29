import streamlit as st
from PIL import Image
import torch
from stqdm import stqdm
from utils import *
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from multiprocessing.pool import ThreadPool 
import os
import uuid
import json
from time import sleep

# Загрузка переменных окружения из .env файла
load_dotenv()


BUCKET = os.getenv('BUCKET')
ORG_LIST = os.getenv('ORG_ID')
IFRAME = '<iframe src="https://ghbtns.com/github-btn.html?user=yandex-cloud-socialtech&repo=spina-bifida&type=watch&count=true&size=large" frameborder="0" scrolling="0" width="121" height="30" title="GitHub"></iframe>'

result = {}

# Настройки страницы
st.set_page_config(
     page_title='Dataset spina bifida uploader',
     layout="wide",
     initial_sidebar_state="expanded",
)

###########
# Функции #
###########

# Функция для отображения модального окна с условиями использования
def show_terms_modal():
    st.write('## Spina Bifida dataset uploader')
    st.markdown(
        f"""
        ### GitHub {IFRAME}
        """,
        unsafe_allow_html=True,
    )
    st.write("Please read and accept our terms of use of this service to continue.")
    st.markdown("---")
    st.write("""
        **Legal disclaimer:** 
        The authors of this web service have made every effort to ensure the accuracy of the ultrasound analysis of the first trimester. However, information is constantly changing due to continuous research and clinical activities, significant differences of opinion among various organizations, and the unique nature of individual cases.  """)
    st.write("""**The information in this web service is intended exclusively for use by** **:red[specialists with medical education, practicing specialists in the field of ultrasound diagnostics, perinatology]** for scientific purposes and **should not be considered as professional advice or a substitute for consultation with a qualified doctor or other healthcare professional.**
        """)
    st.write("""**:red[It is forbidden]** to upload images containing the patient's personal data to the service. 
        """)
    st.write("""The studies uploaded to the web service will be saved and used to improve the quality of this model. 
        """)
    st.write("""**:red[The result of the service is not a diagnosis, consult a doctor for advice.]**
             """)
    
    accept = st.button("I have read and agree to the above conditions")

    return accept

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
        # st.success(f"Файл {file_name} успешно загружен в Yandex Object Storage.")
        os.remove(file_name) # Удаление локального файла после успешной загрузки
    except Exception as e:
        st.error(f"An error has occurred: {e}")
    except FileNotFoundError:
        st.error("The file was not found.")
    except NoCredentialsError:
        st.error("An error with the credentials.")
    except Exception as e:
        st.error(f"An error has occurred: {e}")

# Функция генерации уникального идентификатора файла
def get_unique_id():
    unique_id = str(uuid.uuid4())
    return unique_id

def upload(uploaded_file): 
    img = Image.open(uploaded_file)
    img_bytes = io.BytesIO()
    img.save(img_bytes, format='PNG')
    img_bytes = img_bytes.getvalue()
    img_name = uploaded_file.name

    uniquie_id = get_unique_id()
    img_file_name = f'img_{uniquie_id}_{img_name}'

    # Временное хранение файлов
    img.save(img_file_name)

    # Запись на S3
    upload_to_yandex_cloud(img_name, img_file_name, BUCKET, f'{img_file_name}')


##############
# Приложение #
##############

# Сайдбар
with st.sidebar:
    org_id = st.text_input("Organization_ID")
    
# Основной контент
# Проверяем, если пользователь уже принял условия
if 'accepted' not in st.session_state:
    st.session_state.accepted = False

if not st.session_state.accepted:
    if show_terms_modal():
        st.session_state.accepted = True
        st.rerun()
else:
    # Заголовок приложения
    st.title('Spina Bifida dataset uploader')
    st.markdown(
        f"""
        ### GitHub {IFRAME}
        """,
        unsafe_allow_html=True,
    )
    if org_id in ORG_LIST and org_id != "":
        # Элемент для загрузки файлов
        uploaded_files = st.file_uploader(
            label="Upload images:", 
            type=["png", "jpg", "jpeg"], 
            accept_multiple_files=True, 
            label_visibility='visible'
        )
        
        if uploaded_files:
            pool = ThreadPool(processes=10) 
            pool.map(upload, uploaded_files) 



            # Вывод информации о загруженных и обработанных файлах
            col1 = st.columns(1)[0]
            with col1:
                st.metric(label='Uploaded', value=len(uploaded_files))

            st.success(f'Thank you for your contribution! To upload more files, press *Ctrl+R* (*Cmd+R*) to restart the application.', icon="✅")


    else:
        st.error('Please enter Organization_ID.')
        st.write('To get an ID, fill out the [form](https://spinabifidacheck.ru/contact)')

