from google.cloud import speech_v1
from google.cloud.speech_v1 import enums
from os import path
import subprocess
import io
import os
import requests
from google.cloud import storage
from util_neo4j import UtilNeo4j
import csv
import wave
from typing import List, Tuple
from multiprocessing import Pool


BUCKET_NAME_VIDEOS = "videos_wordbox"
BUCKET_NAME_AUDIOS = "audios_wordbox"
MESSAGE_UPLOAD_FILE = 'File {} uploaded.'
PATH_LOCAL_VIDEO_MP4 = 'videos/{}.mp4'
PATH_LOCAL_AUDIOS = 'audios/{}.wav'
PATH_CSV = 'result/results_{}.csv'
PATH_CSV_ERROR = 'result/results_{}_error.csv'
MAX_ITEMS = 100
URL_GETYARN = 'https://y.yarn.co/{}.mp4'
LENGUAGE_CODE = "en-US"
SERIES = "series"
MOVIES = "movies"
VIDEO_MODEL = "video"
PHONE_CALL_MODEL = "phone_call"


def download_content_getyarn(content_id: str):
    url = URL_GETYARN.format(content_id)
    request = requests.get(url, allow_redirects=True)
    open(PATH_LOCAL_VIDEO_MP4.format(content_id), 'wb').write(request.content)


def upload_file(path_file_upload: str, file_id, bucket_name: str, name_directory: str):
    # Storage Client GCP
    path_file_upload = os.getcwd() + "/" + path_file_upload
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{name_directory}/{file_id}")
    blob.upload_from_filename(path_file_upload)


def convert_video_to_wav(video_id: str):
    # https://ffmpeg.org/
    subprocess.call(['ffmpeg', '-i', PATH_LOCAL_VIDEO_MP4.format(video_id),
                     PATH_LOCAL_AUDIOS.format(video_id)])


def read_metadata_wav(path_url_wav: str) -> Tuple[int, int]:
    with wave.open(path_url_wav, mode='rb') as metadata:
        return metadata.getnchannels(), metadata.getframerate()


def transcribe_audio_file(audio_id: str, sample_rate_hertz: int = 48000,
                          channels: int = 2, model: str = VIDEO_MODEL) -> Tuple[str, float]:

    client = speech_v1.SpeechClient()

    # The language of the supplied audio
    language_code = LENGUAGE_CODE

    # Encoding of audio data sent. This sample sets this explicitly.
    # This field is optional for FLAC and WAV audio formats.
    encoding = enums.RecognitionConfig.AudioEncoding.LINEAR16
    config = {
        "sample_rate_hertz": sample_rate_hertz,
        "language_code": language_code,
        "encoding": encoding,
        "use_enhanced": True,
        "audio_channel_count": channels,
        "model": model
    }

    with io.open(PATH_LOCAL_AUDIOS.format(audio_id), "rb") as f:
        content = f.read()

    audio = {"content": content}

    response = client.recognize(config, audio)

    transcribe_text = ""
    confidence = 0

    for result in response.results:
        # First alternative is the most probable result
        alternative = result.alternatives[0]
        transcribe_text = alternative.transcript
        confidence = alternative.confidence
        print(u"Audio id 1: {}".format(audio_id))
        print(u"Transcript: {}".format(transcribe_text))
        print(u"confidence: {}".format(confidence))

    return transcribe_text, confidence


def write_csv(line: List[str], index):
    with open(PATH_CSV.format(index), "a") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(line)


def write_csv_error(line: List[str], index):
    with open(PATH_CSV_ERROR.format(index), "a") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(line)


def remove_file(path_file: str):
    os.remove(path_file)


def process(page: int, name_content: str, type: str):
    print("**************************")
    print(f"Processing page => {page}")
    util_neo4j: UtilNeo4j = UtilNeo4j()
    contents = util_neo4j.get_content(name_content, page, MAX_ITEMS)
    util_neo4j.close()
    index = 1
    for content in contents:
        try:
            content_id = content.id
            download_content_getyarn(content_id)
            upload_file(PATH_LOCAL_VIDEO_MP4.format(
                content_id), content_id, BUCKET_NAME_VIDEOS, type)
            convert_video_to_wav(content_id)
            audio_wav = PATH_LOCAL_AUDIOS.format(content_id)
            channels, framerate = read_metadata_wav(audio_wav)
            upload_file(audio_wav,
                        content_id, BUCKET_NAME_AUDIOS, type)
            transcribe_text_video_model, confidence_video_model = transcribe_audio_file(
                content_id, framerate, channels, VIDEO_MODEL)
            transcribe_text_phone_call, confidence_phone_call = transcribe_audio_file(
                content_id, framerate, channels, PHONE_CALL_MODEL)
            line_to_write = [str(index),
                             content_id,
                             content.phrase,
                             transcribe_text_video_model,
                             transcribe_text_phone_call,
                             str(confidence_video_model),
                             str(confidence_phone_call)]
            write_csv(line_to_write, page)
            remove_file(audio_wav)
            remove_file(PATH_LOCAL_VIDEO_MP4.format(
                content_id))
            index = index + 1
        except Exception as error:
            line_to_write = [str(content.id), str(
                content.phrase), str(error)]
            write_csv_error(line_to_write, page)


def start_process_upload_and_transcribe(name_content: str, type: str):
    page = 0
    page_tmp = 0
    final_page = 20
    print("Init process")
    while page_tmp <= final_page:
        try:
            with Pool(5) as pool:
                pool.starmap(process, [
                    (page_tmp, name_content, type),
                    (page_tmp+1, name_content, type),
                    (page_tmp+2, name_content, type),
                    (page_tmp+3, name_content, type),
                    (page_tmp+4, name_content, type)
                ])

            print(f"Page tmp {page_tmp}")
            page = page + 1
            page_tmp = page * 5

        except Exception as error:
            line_to_write = ["ERROR", str(page), str(error)]
            write_csv_error(line_to_write, page)

    print("Finish process")


if __name__ == '__main__':
    start_process_upload_and_transcribe("Two and a Half Men", SERIES)
