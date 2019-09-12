from io import open
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
import shutil
from apscheduler.schedulers.blocking import BlockingScheduler

class File:

    @classmethod
    def main(cls, source):   

        sched = BlockingScheduler()

        #client = MongoClient(Mongo_URI)#Para conectarse con la base de datos
        client = MongoClient(
            f'mongodb+srv://{source["user"]}:{source["password"]}@{source["host"]}/admin?retryWrites=true')

        db = client[source['database']]#Name database

        #Variable para la ruta al directorio
        path = source['url']
    
        
        @sched.scheduled_job('interval', seconds=5)
        def timed_job():
            print('This job is run every five seconds.')              
            #Lista vacia para incluir los ficheros
            lstFiles = []

            #Lista con todos los ficheros del directorio:
            lstDir = os.walk(path)   #os.walk()Lista directorios y ficheros

            try:
                #Crea una lista de los ficheros dat que existen en el directorio y los incluye a la lista.
                for root, dirs, files in lstDir:
                    for fichero in files:
                        (nombreFichero, extension) = os.path.splitext(fichero)
                        if(extension == source['extensionfile']):
                            lstFiles.append(nombreFichero+extension)
            except:
                print('Falla verificando los archivos')                 
            
            try:
                #Recorrer por pares y construir el json con el tamaño de la cabecera (primera linea)
                for j in lstFiles:
                    nameFile=j
                    collection = db[nameFile[17:-4]]

                    with open(path+nameFile, 'r') as file:
                        
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

                                    collection.insert_one({"Json": data })
                                    
                        except ConnectionFailure:
                            print('Cannot connect to Momgo DB')
                        except:
                            print('Falla construyendo el json para la bd')
                        file.close()
                        print('Archivo procesado: '+nameFile)
                        try:
                            os.rename(path+nameFile, path+nameFile+'.proccess')
                            shutil.move(path+nameFile+'.proccess', path+"Procesado/"+nameFile+'.proccess')
                        except:
                            print('Failed move file or rename file')
            except:
                print('Falla construyendo el JSON para la bd')
            
                    
        try:
            #Asignamos el job que se ejecutará en un día a una hora especifica
            #sched.add_job(timed_job, 'interval', seconds=source['seconds'])
            sched.start()
        except:
            print('Error, debes validar las propiedades del tiempo de ejecución')
        

    