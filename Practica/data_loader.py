import time
import pandas as pd
import math

@data_loader
def data_loader(*args, **kwargs):
    retries = kwargs.get("retries", 3)
    chunk_size = kwargs.get("chunk_size", 100000)
    url = kwargs.get("url")

    #reintentos
    for attempt in range(1, retries + 1):
        try:
            df = pd.read_parquet(url)
            break
        except Exception as e:
            if attempt < retries - 1:
                raise RuntimeError(
                    f"No se pudo leer el archivo después de {retries} intentos"
                ) from e
            time.sleep(1)
    #chunking
    total_chunks = math.ceil(len(df)/chunk_size
    chunks = []

    for i in range(total_chunks):
        start = i * chunk_size
        end = start + chunk_size

        chunk = df.iloc[start:end].copy()
        chunks.append(chunk)

    final_df = pd.concat(chunks, ignore_index=True)
    return final_df
