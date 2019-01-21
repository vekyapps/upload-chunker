import os
import time
import logging
from queue import Queue
import hashlib

import requests

import concurrent.futures

SIZE = 5 # Size in bytes
MAX_CHUNK_RETRY_TIMES = 3
CHUNK_EXPIRATION_TIME = 10 # 10 minutes

PARALLEL_CHUNKS = 3
PARALLEL_UPLOADS = 3

logger = logging.getLogger(__name__)

fh = logging.FileHandler('logs/error.log')
fh.setLevel(logging.ERROR)

logger.addHandler(fh)

class ChunkedUploader:
    def __init__(self, **kwargs):
        self.done_chunking = False
        self.uploaded_chunks = []
        self.filenames = Queue()
        for filepath in kwargs['filenames']:
            self.filenames.put(filepath)
            
        self.url = kwargs['url']
    
    def chunk_file(self, filepath):
        if not hasattr(self, 'filenames'):
            raise Exception('Filename is missing')

        if hasattr(self, '_chunks'):
            self._chunks.empty()
        else:
            self._chunks = Queue()

        # Obtain filename hash for identifier    
        filename = filepath.split(os.sep)[-1]
        hash_object = hashlib.sha1(filename.encode('utf-8'))
        filename_hash = hash_object.hexdigest()
        
        with open(filepath, "rb") as f:
            counter = 0
            while True:
                f.seek(counter)
                chunk = f.read(SIZE)
                if len(chunk) == 0:
                    yield None

                yield {
                    "file_id": filename_hash,
                    "chunk_id": counter,
                    "data": chunk,
                    "status": "queued"
                }
                counter += SIZE
                
    def start(self):
        total_threads = PARALLEL_CHUNKS+PARALLEL_UPLOADS 
        with concurrent.futures.ThreadPoolExecutor(max_workers=total_threads) as executor:
            try:
                for i in range(PARALLEL_CHUNKS):
                    r = executor.submit(self._chunker)
                for i in range(PARALLEL_UPLOADS):
                    r = executor.submit(self._uploader)
            except Exception as e:
                logger.error(e)
            finally:
                executor.shutdown(True)
            
    def _reset(self):
        self.done_chunking = False
        self.uploaded_chunks = []

    def _chunker(self):
        self._chunks_in_upload = 0
        while not self.filenames.empty():
            filepath = self.filenames.get()
            chunk_generator = self.chunk_file(filepath)
            while True:
                chunk = next(chunk_generator)
                if chunk == None:
                    break
                self._chunks.put(chunk)
            
        return True
    
    def _uploader(self):
        while not self.filenames.empty() and not self._chunks.empty():
            chunk = self._chunks.get()
            self._chunks_in_upload += 1
            result = self._chunk_upload(chunk)
            if result:
                self._chunks_in_upload -= 1
            else:
                chunk['status'] = 'failed'
                if 'fail_counter' in chunk:
                    chunk['fail_counter'] += 1
                else:
                    chunk['fail_counter'] = 1
                    
                if 'fail_counter' in chunk and chunk['fail_counter'] > MAX_CHUNK_RETRY_TIMES:
                    raise Exception(
                        'Chunk upload failed more than {0} time(s)'
                        .format(MAX_CHUNK_RETRY_TIMES))
                self._chunks.put(chunk)

        return True
        
    def _chunk_upload(self, chunk):
        if not hasattr(self, 'url') or not self.url:
            raise ValueError("URL is not specified")

        expiration_time = int(time.time())+(CHUNK_EXPIRATION_TIME*60)
        r = requests.post(self.url, data = {
            'id': chunk['id'],
            'data': chunk['data'],
            'expires': expiration_time
        })
        
        if not r.status_code == 200:
            return False
        
        if r.json==None or not r.json['success'] == True:
            return False
        
        return True

    
# Test:
test_filenames = ["test1.txt","test2.txt"]
test_url = "http://localhost/chunk_test/chunk.php"
chunker = ChunkedUploader(
    filenames = test_filenames,
    url = test_url
)
chunker.start()
