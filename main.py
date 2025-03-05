import ftplib
import os
from google.cloud import storage

def ftp_to_gcs(event, context):
    """Transfère les fichiers d'un serveur FTP vers Google Cloud Storage."""

    # Informations d'identification FTP
    FTP_HOST = ''  # Remplacez par votre hôte FTP
    FTP_USER = ''  # Remplacez par votre utilisateur FTP
    FTP_PASS = ''  # Remplacez par votre mot de passe FTP

    # Informations Google Cloud Storage
    GCS_BUCKET_NAME = ''  # Remplacez par le nom de votre bucket GCS

    try:
        # Connexion FTP
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(FTP_USER, FTP_PASS)

        # Connexion GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET_NAME)

        # Fonction récursive pour parcourir les répertoires FTP
        def transfer_recursive(ftp_path, gcs_path):
            try:
                ftp.cwd(ftp_path)
                items = ftp.nlst()

                for item in items:
                    ftp_item_path = os.path.join(ftp_path, item)
                    gcs_item_path = os.path.join(gcs_path, item)

                    try:
                        # Si c'est un répertoire, on appelle récursivement
                        ftp.cwd(ftp_item_path)
                        ftp.cwd(ftp_path) #retour au repertoire parent
                        transfer_recursive(ftp_item_path, gcs_item_path)
                    except Exception as e:
                        # Si c'est un fichier, on le télécharge et téléverse
                        print(f"Transfert de {ftp_item_path} vers {gcs_item_path}")
                        with open(item, 'wb') as f:
                            ftp.retrbinary(f"RETR {item}", f.write)
                        blob = bucket.blob(gcs_item_path)
                        blob.upload_from_filename(item)
                        os.remove(item)

            except Exception as e:
                print(f"Erreur lors du traitement de {ftp_path}: {e}")

        # Démarrer le transfert depuis la racine du FTP
        transfer_recursive('.', '')

        ftp.quit()
        print("Transfert FTP vers GCS terminé avec succès.")

    except Exception as e:
        print(f"Erreur lors du transfert: {e}")