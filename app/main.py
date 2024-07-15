from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse
import os
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from app.config import s3_client, bucket_name  # S3 클라이언트와 버킷 이름 설정
from app.database import insert_video_document, find_video_document, update_video_document, find_pending_documents # 데이터베이스 관련 함수
from app.video_processor import process_video  # 비디오 처리 함수
import shutil
import time
import os
import json


app = FastAPI()


# 시크릿 제이슨 파일에서 환경 변수를 로드
with open('app/secrets.json') as f:
    secrets = json.load(f)

FAST_API_MP_PORT= secrets['FAST_API_MP_PORT']


# 작업 큐 생성
work_queue = asyncio.Queue()

# 로그 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ThreadPoolExecutor 설정
executor = ThreadPoolExecutor(max_workers=1)

# 다운로드 경로 설정
DOWNLOAD_PATH = "./downloads"
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

@app.post("/mp-editvideo/")
async def upload_file(
    videofile: UploadFile = File(...),  # 비디오 파일 업로드
    filename: str = Form(...),  # 파일 이름
    worknum: str = Form(...),  # 작업 번호
    power: str = Form(...),  # 파워
    mosaic_strength: str = Form(...),  # 모자이크 강도
):
    logger.info("Upload request received")  # 업로드 요청 수신 로그
    try:
        # 파일 저장 경로 설정
        file_extension = videofile.filename.split('.')[-1]
        file_location = os.path.join(DOWNLOAD_PATH, f"{worknum}.{file_extension}")
        
        # 파일 저장
        with open(file_location, "wb") as file_object:
            shutil.copyfileobj(videofile.file, file_object)
        logger.info(f"File saved at {file_location}")  # 파일 저장 로그

        # MongoDB에 데이터 저장
        document = {
            "worknum": worknum,
            "video_file_path": file_location,
            "filename": filename,
            "knife": 0,
            "gun": 0,
            "cigarrete": 0,
            "middle_finger": 0,
            "credit_card": 0,
            "receipt": 0,
            "license_plate": 0,
            "job_ok": 0,
            "s3_url": "",
            "power": power,
            "mosaic_strength": mosaic_strength,
        }
        
        insert_video_document(document)  # 문서 삽입
        logger.info("Document inserted into MongoDB")  # MongoDB 삽입 로그

        # 작업을 큐에 추가
        await work_queue.put(worknum)
        logger.info(f"Added {worknum} to the queue")  # 작업 큐에 추가 로그

        # 즉시 응답 반환
        return JSONResponse(content={"message": 200})
    except Exception as e:
        logger.error(f"Error in upload_file: {e}")  # 오류 로그
        raise HTTPException(status_code=500, detail="Upload failed")

@app.get("/mp-downloadvideo/")
async def get_download_link(worknum: str):
    try:
        # 작업 번호로 문서 조회
        document = find_video_document(worknum)
        
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{worknum}/")
        
        if 'Contents' not in response:
            raise HTTPException(status_code=404, detail="Job number folder not found")
        
        mp4_files = [content['Key'] for content in response['Contents'] if content['Key'].endswith('.mp4')]
        
        if not mp4_files:
            raise HTTPException(status_code=404, detail="No mp4 files found in the job number folder")

        mp4_file_key = mp4_files[0]
        download_url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': bucket_name, 'Key': mp4_file_key},
            ExpiresIn=3600  # URL 유효 기간: 1시간 (3600초)
        )

        

        return {
            "download_url": download_url,
            "labels": {
                "knife": str(document.get("knife", 0)),
                "gun": str(document.get("gun", 0)),
                "cigarrete": str(document.get("cigarrete", 0)),
                "middle_finger": str(document.get("middle_finger", 0)),
                "credit_card": str(document.get("credit_card", 0)),
                "receipt": str(document.get("receipt", 0)),
                "license_plate": str(document.get("license_plate", 0))
            }
        }

    except Exception as e:
        logger.error(f"Error in get_download_link: {e}")  # 오류 로그
        raise HTTPException(status_code=500, detail=str(e))

async def video_processing_worker():
    while True:
        logger.info("Worker waiting for a task")  # 작업 대기 중 로그
        worknum = await work_queue.get()
        logger.info(f"Worker got task: {worknum}")  # 작업 수신 로그
        try:
            logger.info(f"Worker started processing worknum: {worknum}")  # 작업 시작 로그
            document = find_video_document(worknum)

            if document and document["job_ok"] == 0:
                video_file_path = document['video_file_path']
                filename = document['filename']
                power = document['power']
                mosaic_strength = document.get('mosaic_strength', 15)
                logger.info(f"Processing worknum: {worknum}, video path: {video_file_path}")  # 비디오 처리 시작 로그

                # 작업 상태 업데이트: 시작
                update_video_document(worknum, {"job_ok": 1})

                mosaic_strength = int(mosaic_strength)
                starttime = time.time()
                
                # ThreadPoolExecutor를 사용하여 비디오 처리 작업을 비동기적으로 실행
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(executor, process_video, worknum, video_file_path, filename, power, mosaic_strength)
                
                endtime = time.time()

                logger.info(f"Processing time for {worknum}: {endtime - starttime:.2f} seconds")  # 처리 시간 로그

                # 작업 상태 업데이트: 완료
                update_video_document(worknum, {"job_ok": 2})
        except Exception as e:
            logger.error(f"Error processing {worknum}: {e}")  # 오류 로그
            # 작업 상태 업데이트: 에러
            update_video_document(worknum, {"job_ok": -1, "error": str(e)})
        finally:
            work_queue.task_done()
            logger.info(f"Worker finished processing worknum: {worknum}")  # 작업 완료 로그

@app.get("/mp-findlist/")
async def get_pending_jobs():
    try:
        pending_jobs = find_pending_documents()
        return JSONResponse(content={"pending_jobs": pending_jobs})
    except Exception as e:
        logger.error(f"Error in get_pending_jobs: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve pending jobs")

    

@app.on_event("startup")
async def startup_event():
    logger.info("Starting video processing worker")  # 워커 시작 로그
    asyncio.create_task(video_processing_worker())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=FAST_API_MP_PORT, reload=True)
