"""
Taller presencial - MapReduce en Python para conteo de palabras

Este script implementa un flujo MapReduce para contar la frecuencia de palabras en archivos de texto.
Utiliza procesamiento paralelo con multiprocessing para optimizar el rendimiento.
"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import itertools
import os
import os.path
import string
import time
from collections import defaultdict
from multiprocessing import Pool

from toolz.itertoolz import concat  # type: ignore


# ------------------------------------
# Copia archivos crudos a la carpeta input
# ------------------------------------
def copy_raw_files_to_input_folder(n):
    """Copia 'n' veces los archivos desde files/raw a files/input."""

    create_directory(directory="files/input")

    for file in glob.glob("files/raw/*"):
        for i in range(1, n + 1):
            with open(file, "r", encoding="utf-8") as f:
                with open(
                    f"files/input/{os.path.basename(file).split('.')[0]}_{i}.txt",
                    "w",
                    encoding="utf-8",
                ) as f2:
                    f2.write(f.read())

# ------------------------------------
# Lectura de archivos
# ------------------------------------
def load_input(input_directory):
    """
    Carga líneas de texto desde un archivo o múltiples archivos en un directorio.

    Retorna un generador que produce líneas de texto desde uno o varios archivos.
    """

    def make_iterator_from_single_file(input_directory):
        with open(input_directory, "rt", encoding="utf-8") as file:
            yield from file

    def make_iterator_from_multiple_files(input_directory):
        input_directory = os.path.join(input_directory, "*")
        files = glob.glob(input_directory)
        with fileinput.input(files=files) as file:
            yield from file

    if os.path.isfile(input_directory):
        return make_iterator_from_single_file(input_directory)

    return make_iterator_from_multiple_files(input_directory)

# ------------------------------------
# Preprocesamiento
# ------------------------------------
def preprocessing(x):
    """Convierte la línea a minúsculas, elimina puntuación y saltos de línea."""
    x = x.lower()
    x = x.translate(str.maketrans("", "", string.punctuation))
    x = x.replace("\n", "")
    return x

def line_preprocessing(sequence):
    """Preprocesa en paralelo todas las líneas del input."""
    with Pool() as pool:
        return pool.map(preprocessing, sequence)

# ------------------------------------
# Mapper
# ------------------------------------
def map_line(x):
    """Convierte una línea en una lista de tuplas (palabra, 1)."""
    return [(w, 1) for w in x.split()]

def mapper(sequence):
    """Aplica map_line en paralelo a cada línea y concatena los resultados."""
    with Pool() as pool:
        sequence = pool.map(map_line, sequence)
        sequence = concat(sequence)
    return sequence

# ------------------------------------
# Shuffle and Sort
# ------------------------------------
def shuffle_and_sort(sequence):
    """Ordena la secuencia por clave (palabra) para agrupar posteriormente."""
    return sorted(sequence, key=lambda x: x[0])

# ------------------------------------
# Reducer
# ------------------------------------
def sum_by_key(chunk):
    """Suma los valores por clave en un chunk de datos."""
    result = defaultdict(int)
    for key, value in chunk:
        result[key] += value
    return list(result.items())

def reducer(sequence):
    """
    Agrupa y reduce en paralelo la secuencia de pares (clave, valor),
    sumando los valores por clave.
    """

    def chunkify(sequence, num_chunks):
        return [sequence[i::num_chunks] for i in range(num_chunks)]

    def merge_results(chunks):
        final_result = defaultdict(int)
        for chunk in chunks:
            for key, value in chunk:
                final_result[key] += value
        return list(final_result.items())

    num_chunks = os.cpu_count()
    chunks = chunkify(sequence, num_chunks)

    with Pool(num_chunks) as pool:
        chunk_results = pool.map(sum_by_key, chunks)

    return merge_results(chunk_results)

# ------------------------------------
# Crea directorio de salida
# ------------------------------------
def create_directory(directory):
    """Crea un directorio limpio, eliminando el anterior si ya existe."""
    if os.path.exists(directory):
        for file in glob.glob(f"{directory}/*"):
            os.remove(file)
        os.rmdir(directory)
    os.makedirs(directory)

# ------------------------------------
# Guarda resultado final
# ------------------------------------
def save_output(output_directory, sequence):
    """Guarda el resultado en un archivo TSV (clave y valor separados por tabulación)."""
    with open(f"{output_directory}/part-00000", "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")

# ------------------------------------
# Marca éxito del proceso
# ------------------------------------
def create_marker(output_directory):
    """Crea un archivo _SUCCESS para indicar que el trabajo fue completado."""
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")

# ------------------------------------
# Ejecución del flujo completo MapReduce
# ------------------------------------
def run_job(input_directory, output_directory):
    """
    Orquesta la ejecución completa del proceso MapReduce:
    carga, preprocesamiento, mapeo, ordenamiento, reducción y guardado.
    """
    sequence = load_input(input_directory)
    sequence = line_preprocessing(sequence)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)

    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)

# ------------------------------------
# Punto de entrada del programa
# ------------------------------------
if __name__ == "__main__":
    # Copia archivos de ejemplo
    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecuci\u00f3n: {end_time - start_time:.2f} segundos")
