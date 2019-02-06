config = {
    'SILENT_FILE_NOT_EXISTS': False,
    'SILENT_FILE_NOT_READABLE': False,

    'ASYNC_UPLOAD': False,
    'PARALLEL_CHUNKS': 1,
    'PARALLEL_UPLOADS': 1,

    'CHUNK_SIZE': 5,  # Size in bytes
    'MAX_CHUNK_RETRY_TIMES': 3,
    'CHUNK_EXPIRATION_TIME': 10,  # Duration in minutes
    'CHUNK_EXPIRATION_DATETIME_FORMAT': None,
    'CHUNK_LOG_LEVEL': 1
}