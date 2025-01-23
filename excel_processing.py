import pandas as pd
import asyncio
import logging

# Logger konfigurieren
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExcelExporter:
    @staticmethod
    async def export_to_excel(db_pool, output_file):
        """
        Exportiert Daten aus der Datenbank in eine Excel-Datei. Schreibt alle Spalten aus dem Mapping
        in die Excel-Datei, auch wenn sie in der Datenbank leer sind. Leere Felder bleiben leer.
        """
        try:
            # Daten aus der Datenbank abfragen
            logger.info("Beginne mit der Abfrage der Tabelle `library`.")
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM library")
            logger.info(f"Abfrage abgeschlossen. Anzahl der Zeilen: {len(rows)}.")

            # Daten in ein Pandas DataFrame umwandeln
            db_df = pd.DataFrame([dict(row) for row in rows])
            logger.info("Daten erfolgreich in DataFrame konvertiert.")
            logger.debug(f"DataFrame vor dem Mapping:\n{db_df.head()}")

            # Spalten-Mapping
            columns_mapping = {
                "action": "Action(SiteID=Germany|Country=DE|Currency=EUR|Version=1193)",
                "id": "Custom label (SKU)",
                " ": "Category name",
                "title": "Title",
                " ": "Relationship",
                " ": "Relationship details",
                " ": "Schedule Time",
                "isbn": "P:ISBN",
                " ": "P:EPID",
                "Start_price": "Start price",
                "Quantity": "Quantity",
                "photo": "Item photo URL",
                "": "VideoID",
                "Condition_ID": "Condition ID",
                "Description": "Description",
                "Format": "Format",
                "Duration": "Duration",
                "": "Buy It Now price",
                "Best_Offer_Enabled": "Best Offer Enabled",
                "Best_Offer_Auto_Accept_Price": "Best Offer Auto Accept Price",
                "Minimum_Best_Offer_Price": "Minimum Best Offer Price",
                "": "VAT%",
                "": "Immediate pay required",
                "Location": "Location",
                "": "Shipping service 1 option",
                "": "Shipping service 1 cost",
                "": "Shipping service 1 priority",
                "": "Shipping service 2 option",
                "": "Shipping service 2 cost",
                "": "Shipping service 2 priority",
                "": "Max dispatch time",
                "": "Returns accepted option",
                "": "Returns within option",
                "": "Refund option",
                "": "Return shipping cost paid by",
                "Shipping_profile_name": "Shipping profile name",
                "Return_profile_name": "Return profile name",
                "Payment_profile_name": "Payment profile name",
                "": "ProductCompliancePolicyID",
                "": "Regional ProductCompliancePolicies",
                "Autor": "C:Autor",
                "Buchtitel": "C:Buchtitel",
                "Sprache": "C:Sprache",
                "": "C:Thematik",
                "": "C:Buchreihe",
                "": "C:Genre",
                "Verlag": "C:Verlag",
                "Erscheinungsjahr": "C:Erscheinungsjahr",
                "": "C:Originalsprache",
                "CFormat": "C:Format",
                "": "C:Herstellungsland und -region",
                "Produktart": "C:Produktart",
                "": "C:Literarische Gattung",
                "": "C:Zielgruppe",
                "Signiert_von": "C:Signiert von",
                "Ausgabe": "C:Ausgabe",
                "": "C:Literarische Bewegung",
                "": "Product Safety Pictograms",
                "": "Product Safety Statements",
                "": "Product Safety Component",
                "": "Regulatory Document Ids",
                "": "Manufacturer Name",
                "": "Manufacturer AddressLine1",
                "": "Manufacturer AddressLine2",
                "": "Manufacturer City",
                "": "Manufacturer Country",
                "": "Manufacturer PostalCode",
                "": "Manufacturer StateOrProvince",
                "": "Manufacturer Phone",
                "": "Manufacturer Email",
                "": "Manufacturer ContactURL",
                "": "Responsible Person 1",
                "": "Responsible Person 1 Type",
                "": "Responsible Person 1 AddressLine1",
                "": "Responsible Person 1 AddressLine2",
                "": "Responsible Person 1 City",
                "": "Responsible Person 1 Country",
                "": "Responsible Person 1 PostalCode",
                "": "Responsible Person 1 StateOrProvince",
                "": "Responsible Person 1 Phone",
                "": "Responsible Person 1 Email",
                "": "Responsible Person 1 ContactURL",
            }

            # Erstelle einen leeren DataFrame mit allen Excel-Spalten
            excel_columns = list(columns_mapping.values())
            final_df = pd.DataFrame(columns=excel_columns)

            # Fülle den DataFrame mit Daten aus der Datenbank (falls vorhanden)
            for db_col, excel_col in columns_mapping.items():
                if db_col in db_df.columns:
                    final_df[excel_col] = db_df[db_col]
                else:
                    final_df[excel_col] = ""  # Feld bleibt leer

            logger.info("Alle Spalten erfolgreich im finalen DataFrame erstellt.")

            # Überprüfe, ob DataFrame leer ist
            if final_df.empty:
                logger.warning("Der finale DataFrame ist leer. Keine Daten zum Exportieren.")
                return

            # Schreibe die Daten in eine Excel-Datei
            final_df.to_excel(output_file, index=False, sheet_name="Listings")
            logger.info(f"Daten erfolgreich in {output_file} exportiert.")

        except Exception as e:
            logger.error(f"Fehler beim Exportieren der Daten: {e}")
