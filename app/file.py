'''Clase con la lógica implementada, tomar n archivos y llevar a bd mongo'''
from io import open
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from apscheduler.schedulers.blocking import BlockingScheduler
from azure.storage.blob import BlockBlobService, PublicAccess

class File:
    '''Clase principal'''
    @classmethod
    def main(cls, source):
        '''Metodo principal, recibe un json source contenedor de las propiedades'''
        sched = BlockingScheduler()
        block_blob_service = BlockBlobService(
            account_name=source['accountnameazure'],
            account_key=source['accountkeyazure'])

        local_path = os.path.expanduser("~\\")

        #client = MongoClient('mongodb://localhost')#Para conectarse con la base de datos
        client = MongoClient(
            f'mongodb+srv://{source["user"]}:{source["password"]}@{source["host"]}/admin?retryWrites=true')

        db_mongo = client[source['database']]#Name database

        #Variable para la ruta al directorio
        #path = source['url']

        #Asignamos el job que se ejecutará en un día a una hora especifica o un intervalo de tiempo
        @sched.scheduled_job('interval', seconds=source['seconds'])
        def timed_job():
            print('This job is run every five seconds.')
            #Lista vacia para incluir los ficheros
            lst_files = []

            #Lista con todos los ficheros del directorio:
            #lstDir = os.walk(path)   #os.walk()Lista directorios y ficheros

            generator = block_blob_service.list_blobs(source['blobnameazure'])

            try:
                #Crea una lista de los ficheros dat que existen en el directorio
                # y los incluye a la lista.
                for blob in generator:
                    if blob.name[-4:] == source['extensionfile']:
                        lst_files.append(blob.name)
            except:
                print('Falla verificando los archivos')

            try:
                #Recorrer por pares y construir el json con el tamaño de la cabecera (primera linea)
                for j in lst_files:
                    name_file = j
                    block_blob_service.get_blob_to_path(source['blobnameazure'], name_file, local_path+name_file)
                    collection = db_mongo[name_file[17:-4]]

                    with open(local_path+name_file, 'r') as file:

                        try:
                            for i, line in enumerate(file):
                                data = {}
                                data_split = line.split('|')  # Flat file line size
                                if i == 0:
                                    cabecera = line.split('|')

                                if i > 0:
                                    if len(data_split) > 0:
                                        data['Flag'] = '0'
                                        for x in range(len(cabecera)):
                                            data[cabecera[x].rstrip('\n')] = data_split[x].rstrip('\n')

                                    collection.insert_one({"Json" : data})

                        except ConnectionFailure:
                            print('Cannot connect to Momgo DB')
                        except Exception as e:
                            print('Falla construyendo el json para la bd '+str(e))
                        file.close()
                        print('Archivo procesado: '+name_file)
                        try:
                            os.remove(local_path+name_file)
                            blob_url = block_blob_service.make_blob_url(source['blobnameazure'], name_file)
                            block_blob_service.copy_blob(source['blobnameazureprocesados'], name_file, blob_url)
                            block_blob_service.delete_blob(source['blobnameazure'], name_file)
                        except Exception as e:
                            print('Falla moviendo y eliminando el archivo '+str(e))

            except Exception as e:
                print('Falla construyendo el JSON para la bd'+str(e))

        try:
            #sched.add_job(timed_job, 'interval', seconds=source['seconds'])
            sched.start()
        except:
            print('Error, debes validar las propiedades del tiempo de ejecución')
    