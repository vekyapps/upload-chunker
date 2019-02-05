import os
import time
import logging
from queue import Queue
import hashlib
from pathlib import Path
import configparser

import threading

import requests

SIZE = 5  # Size in bytes
MAX_CHUNK_RETRY_TIMES = 3
CHUNK_EXPIRATION_TIME = 10  # 10 minutes

PARALLEL_CHUNKS = 3
PARALLEL_UPLOADS = 3

CHUNK_EXPIRATION_DATETIME_FORMAT = None  # If None -

# Chunk log level
# Possible levels:
#     1 - log only if chunk upload exceed MAX_CHUNK_RETRY_TIMES
#     2 - log every chunk failed upload
#     3 - log every chunk upload
#     4 - log every callback call
CHUNK_LOG_LEVEL = 1

logger = logging.getLogger(__name__)

# TODO
# Callback types:
# 1. before_setup
# 2. after_setup
# 3. filepath_not_exists
# 4. filepath_not_readable
# 5. before_chunking
# 6. after_chunking
# 7. before_chunk
# 8. after_chunk
# 9. before_chunk_upload
# 10. after_chunk_upload
# 11. chunk_upload_failed

# TODO: stop,pause callbacks

fh = logging.FileHandler('logs/error.log')
fh.setLevel(logging.INFO)
logger.addHandler(fh)


class ChunkedUploader:
    def __init__(self, **kwargs):
        self.setup(kwargs)

    def setup(self, **options):
        self._execute_callback_operation('before_setup', self)

        if 'url' not in options:
            raise ValueError('Url must be specified in initialization!')

        self._url = options['url']

        if 'filepaths' not in options:
            raise ValueError('Filepath(s) must be specified!')

        if not isinstance(options['filepaths'], list) or len(options['filepaths']) < 1:
            raise TypeError('Filepath(s) argument must be type of list and must contain at lease one filepath')

        if 'config_ini_filepath' in options:
            config_ini_filepath = options['config_ini_filepath']
            custom_config_file = True
        else:
            config_ini_filepath = 'config.ini'
            custom_config_file = False

        path = Path(config_ini_filepath)
        if not path.exists() or not path.is_file():
            raise IOError('Config filepath %s does not exists!' % config_ini_filepath)
        if not os.access(config_ini_filepath, os.R_OK):
            raise IOError('Config filepath %s is not readable!' % config_ini_filepath)

        config = configparser.ConfigParser()
        config.read(config_ini_filepath)

        self._chunking = True

        self._total_chunked = 0
        self._total_uploaded = 0

        self._start_time = None
        self._end_time = None

        # "fake" async, main thread continues with its execution, after start() method is called!
        if 'async_upload' in options and options['async_upload']:
            self._async = True
        else:
            self._async = False

        self._filenames = Queue()
        self._chunks = Queue()

        if custom_config_file:
            pass


        if 'silent_file_not_exists' in options:
            silent_file_not_exists = options['silent_file_not_exists']
        else:
            silent_file_not_exists = SILENT_FILE_NOT_EXISTS

        if 'silent_file_not_readable' in options:
            silent_file_not_readable = options['silent_file_not_readable']
        else:
            silent_file_not_readable = SILENT_FILE_NOT_READABLE

        for filepath in options['filepaths']:
            path = Path(filepath)
            if not path.exists() or not path.is_file():
                self._execute_callback_operation('filepath_not_exists', self, filepath)
                if silent_file_not_exists:
                    continue
                else:
                    raise FileNotFoundError('File not found, path: %s' % filepath)
                # todo raise error -> + silent
            if not os.access(filepath, os.R_OK):
                self._execute_callback_operation('filepath_not_readable', self, filepath)
                # todo raise error -> + silent

            filesize = os.path.getsize(filepath)

            # Obtain filename hash for identifier
            filename = filepath.split(os.sep)[-1]
            hash_object = hashlib.sha1(filename.encode('utf-8'))
            filename_hash = hash_object.hexdigest()

            self._filenames.put({
                'filepath': filepath,
                'offset_done': 0,
                'filename_hash': filename_hash,
                'file_size': filesize,
                'file_object': open(filepath, "rb")
            })

        if 'logger' in options['logger']:
            if not isinstance(options['logger'], logger):
                raise TypeError('Logger must be type of logging.Logger!')
            self._logger = options['logger']
        else:
            self._logger = None

        if 'callbacks' in options['callbacks']:
            if not isinstance(options['callbacks'], dict):
                raise ValueError('Callbacks option must be type of dict!')
            self._callbacks = options['callbacks']
        else:
            self._callbacks = None

        self._execute_callback_operation('after_setup', self)

    def _execute_callback_operation(self, operation, chunked_uploader, *args):
        if not self._callbacks or operation not in self._callbacks:
            return None

        if not callable(self._callbacks[operation]):
            raise TypeError('Callback operation %s is not callable' % operation)

        return self._callbacks[operation](chunked_uploader, *args)

    def _get_file_chunk(self, file_obj, filename_hash, filename, offset=0):
        if not file_obj:
            raise ValueError('Invalid filepath provided: %s' % file_obj)

        with file_obj as f:
            f.seek(offset)
            chunk = f.read(SIZE)
            if len(chunk) == 0:
                yield None

            yield {
                "file_id": filename_hash,
                "chunk_id": offset,
                "filename": filename,
                "data": chunk,
                "status": "queued",
                "next_chunk": offset+SIZE
            }
            offset += SIZE

    def start(self):
        threads = []
        self._start_time = int(time.time())
        self._execute_callback_operation('before_chunking', self)
        for i in range(PARALLEL_CHUNKS):
            r = threading.Thread(name="chunker" + str(i), target=self._chunker)
            threads.append(r)
        for i in range(PARALLEL_UPLOADS):
            r = threading.Thread(name="uploader" + str(i), target=self._uploader)
            threads.append(r)

        for thread in threads:
            thread.start()

        if not self._async:
            for thread in threads:
                thread.join()

        return True

    def restart(self):
        pass # todo

    def stop(self):
        self._stop = True

    def pause(self):
        self._pause = True

    def get_progress(self):
        return {
            'done': self.is_done(),
            'paused': self.is_paused(),
            'total_chunked': self._total_chunked,
            'total_uploaded': self._total_uploaded,
            'start_time':(self._start_time if self._start_time else None),
            'end_time': (self._end_time if self._end_time else None)
        }

    def is_done(self):
        return (self._total_chunked == self._total_uploaded)

    def is_paused(self):
        return self.is_paused

    def _reset(self):
        self._chunking = True
        self._stop = False
        self._pause = False

    def _chunker(self):
        while not self._filenames.empty():
            filepath = self._filenames.get()
            chunk_generator = self._get_file_chunk(filepath[0])
            while True:
                chunk = next(chunk_generator)
                if chunk:
                    self._filenames.put() ## tu sam stao!
                else:
                    break
                self._chunks.put(chunk)
                self._total_chunked += 1
                ##########print("Stavljam: "+str(chunk))

        self._chunking = False
        self._execute_callback_operation('after_chunking', self)
        return True

    def _uploader(self):
        while self._chunking or not self._chunks.empty():
            chunk = self._chunks.get()
            result = self._chunk_upload(chunk)
            #######print("Skidam: " + str(chunk)+ " REZ: "+str(result))
            if result:
                self._total_uploaded += 1
            else:
                chunk['status'] = 'failed'
                if 'fail_counter' in chunk:
                    chunk['fail_counter'] += 1
                else:
                    chunk['fail_counter'] = 1

                if 'fail_counter' in chunk and chunk['fail_counter'] > MAX_CHUNK_RETRY_TIMES:
                    raise Exception(
                        'Chunk upload failed more than {0} time(s)'.format(MAX_CHUNK_RETRY_TIMES))
                self._chunks.put(chunk)

        if not self._end_time:
            self._end_time = int(time.time())

        return True

    def _chunk_upload(self, chunk):
        expiration_time = int(time.time()) + (CHUNK_EXPIRATION_TIME * 60)
        chunk['expires'] = expiration_time
        self._execute_callback_operation('before_chunk_upload', self, chunk)
        request = requests.post(self.url, data={
            'file_id': chunk['file_id'],
            'chunk_id': chunk['chunk_id'],
            'data': chunk['data'],
            'expires': expiration_time
        })

        if not request.status_code == 200:
            self._execute_callback_operation('chunk_upload_failed', self, chunk)
            return False

        # data = r.json()

        # if data==None or not data['success'] == True:
        #    return False

        self._execute_callback_operation('after_chunk_upload', self, chunk)
        return True


# Test #1 - sync
test_filenames = ["test1.txt", "test2.txt"]
test_url = "http://localhost/chunk_test/chunk.php"
chunker = ChunkedUploader(
    filepaths=test_filenames,
    url=test_url
    # , logger
)
chunker.start()
print("SYNC - GOTOVO!")

# Test #2 - async
test_filenames = ["test1.txt", "test2.txt"]
test_url = "http://localhost/chunk_test/chunk.php"
chunker = ChunkedUploader(
    filepaths=test_filenames,
    url=test_url,
    async_upload=True
    # , logger
)
chunker.start()
while True:
    result = chunker.get_progress()
    print(result)
    if result['done']:
        break
print("ASYNC - GOTOVO!")