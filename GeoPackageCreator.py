from osgeo import ogr, osr
import os
from typing import List, Dict, Any
from connect_db import DatabaseConnect
from pathlib import Path

class GeoPackageCreator:
    def __init__(self, connect_db, output_gpkg: str):
        self.connect_db:DatabaseConnect = connect_db
        self.output_gpkg = output_gpkg
        self.geometry_types = {
            'POINT': ogr.wkbPoint,
            'LINESTRING': ogr.wkbLineString,
            'POLYGON': ogr.wkbPolygon,
            'MULTIPOINT': ogr.wkbMultiPoint,
            'MULTILINESTRING': ogr.wkbMultiLineString,
            'MULTIPOLYGON': ogr.wkbMultiPolygon
        }
        
    def read_layers_info(self) -> List[Dict[str, str]]:
        # Questo metodo dovrebbe essere adattato per lavorare con il tuo connect_db
        return self.connect_db.get_layers_geometry_type('', "SDI")

    def get_geometry_type(self, type_str: str) -> int:
        """Converte la stringa del tipo di geometria nel tipo OGR corrispondente"""
        if not type_str:  # Se TYPE è None o stringa vuota
            return None
        return self.geometry_types.get(type_str.upper(), ogr.wkbUnknown)

    def create_table(self, data_source: ogr.DataSource, table_name: str) -> bool:
        """Crea una tabella non spaziale nel GeoPackage"""
        # Crea una tabella non spaziale
        try:
            # SQL per creare una tabella
            # sql = f"""
            #     CREATE TABLE {table_name} (
            #         fid INTEGER PRIMARY KEY AUTOINCREMENT,
            #         owner TEXT
            #     )
            # """
            sql = f"""
                CREATE TABLE {table_name} (
                    fid INTEGER PRIMARY KEY AUTOINCREMENT
                )
            """
            data_source.ExecuteSQL(sql)
            
            # # Inserisci il valore owner
            # sql_insert = f"""
            #     INSERT INTO {table_name} (owner) VALUES ('{owner}')
            # """
            # data_source.ExecuteSQL(sql_insert)
            
            return True
        except Exception as e:
            print(f"Errore durante la creazione della tabella {table_name}: {str(e)}")
            return False

    def create_spatial_layer(self, data_source: ogr.DataSource, layer_name: str, 
                            geom_type: int, srs: osr.SpatialReference) -> bool:
        """Crea un layer spaziale nel GeoPackage"""
        try:
            # Se il layer esiste già, rimuovilo
            if data_source.GetLayerByName(layer_name) is not None:
                data_source.DeleteLayer(layer_name)

            srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            
            # Crea il nuovo layer con opzioni specifiche per GeoPackage
            options = [
                'GEOMETRY_NAME=geom',  # Nome standard per la colonna geometrica
                'FID=fid',             # Nome standard per la colonna ID
                'SPATIAL_INDEX=YES'    # Crea un indice spaziale
            ]
            
            # Crea il nuovo layer
            layer = data_source.CreateLayer(
                layer_name, 
                srs, 
                geom_type,
                options=options
            )

            if layer is None:
                return False

            # Aggiorna manualmente gpkg_geometry_columns
            success = self.update_geometry_columns(data_source, layer_name, geom_type, srs)
            if not success:
                print(f"Attenzione: impossibile aggiornare gpkg_geometry_columns per {layer_name}")

            # Verifica che l'aggiornamento sia stato effettuato correttamente
            self.verify_geometry_columns(data_source, layer_name)

            # Forza l'aggiornamento delle tabelle di metadati del GeoPackage
            layer.GetExtent(force=1)
            return True
        except Exception as e:
            print(f"Errore durante la creazione del layer spaziale {layer_name}: {str(e)}")
            return False

    def verify_geometry_columns(self, data_source: ogr.DataSource, layer_name: str):
        """Verifica il contenuto di gpkg_geometry_columns per il layer specificato"""
        sql = f"SELECT * FROM gpkg_geometry_columns WHERE table_name = '{layer_name}'"
        result = data_source.ExecuteSQL(sql)
        if result:
            feature = result.GetNextFeature()
            if feature:
                print(f"Verifica gpkg_geometry_columns per {layer_name}:")
                for field, value in feature.items().items():
                    print(f"  {field}: {value}")
            else:
                print(f"Nessun record trovato in gpkg_geometry_columns per {layer_name}")
            data_source.ReleaseResultSet(result)

    def create_gpkg_layers(self, EPSG:int = 32632) -> List[str]:
        # Registra tutti i driver
        ogr.RegisterAll()

        # Crea o apri il GeoPackage
        driver = ogr.GetDriverByName("GPKG")
        if os.path.exists(self.output_gpkg):
            data_source = driver.Open(self.output_gpkg, 1)  # 1 = modalità scrittura
        else:
            data_source = driver.CreateDataSource(self.output_gpkg)

        if data_source is None:
            raise Exception(f"Creazione del GeoPackage fallita: {self.output_gpkg}")

        # Crea il sistema di riferimento (WGS 84)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(EPSG)

        created_items = []
        layers_info = self.read_layers_info()

        for layer_info in layers_info:
            layer_name, geom_type_str = layer_info
            
            try:
                # Determina se è una tabella o un layer spaziale
                if not geom_type_str:
                    # Crea una tabella non spaziale
                    if self.create_table(data_source, layer_name):
                        created_items.append(f"Tabella: {layer_name}")
                else:
                    # Crea un layer spaziale
                    geom_type = self.get_geometry_type(geom_type_str)
                    if geom_type is not None:
                        if self.create_spatial_layer(data_source, layer_name, geom_type, srs):
                            created_items.append(f"Layer spaziale: {layer_name}")

            except Exception as e:
                print(f"Errore durante la creazione di {layer_name}: {str(e)}")

        # Chiudi il data source
        data_source = None

        return created_items
    
    def update_geometry_columns(self, data_source: ogr.DataSource, layer_name: str, 
                               geom_type: int, srs: osr.SpatialReference):
        """Aggiorna la tabella gpkg_geometry_columns con le informazioni corrette"""
        srs_id = srs.GetAuthorityCode(None)
        if not srs_id:
            print(f"Attenzione: impossibile ottenere srs_id per {layer_name}")
            return False

        geom_type_name = ogr.GeometryTypeToName(geom_type)
        
        try:
            sql = f"""
                INSERT OR REPLACE INTO gpkg_geometry_columns 
                (table_name, column_name, geometry_type_name, srs_id, z, m) 
                VALUES ('{layer_name}', 'geom', '{geom_type_name}', {srs_id}, 0, 0)
            """
            data_source.ExecuteSQL(sql)
            return True
        except Exception as e:
            print(f"Errore durante l'aggiornamento di gpkg_geometry_columns per {layer_name}: {str(e)}")
            return False

def main():
    # Assicurati che le eccezioni GDAL vengano restituite come eccezioni Python
    ogr.UseExceptions()
    
    try:
        # Esempio di utilizzo
        db_path = Path("/Users/IG56001/Desktop/Sviluppo Software/GPKG creator/resources/db/db.gpkg")
        connect_db = DatabaseConnect(db_path)  # La tua classe per la connessione al database
        output_gpkg = "1001.gpkg"
        
        creator = GeoPackageCreator(connect_db, output_gpkg)
        created_items = creator.create_gpkg_layers()
        
        print("Layer creati:")
        for item in created_items:
            print(f"- {item}")
    
    except Exception as e:
        print(f"Si è verificato un errore: {str(e)}")

if __name__ == '__main__':
    main()