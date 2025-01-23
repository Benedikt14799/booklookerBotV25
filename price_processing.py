import re
from decimal import Decimal, ROUND_HALF_UP
import logging

logger = logging.getLogger(__name__)

class PriceProcessing:
    """
    Klasse: PriceProcessing
    ------------------------
    Verwaltet die Extraktion, Berechnung und Speicherung von Preisen und relevanten
    Feldern wie 'Minimum Best Offer Price' und 'Best Offer Auto Accept Price'.
    """

    # eBay-Gebühren und zusätzliche Kosten
    EBAY_PERCENTAGE_FEE = Decimal('0.12')    # eBay-Provisionssatz (12%)
    EBAY_FIXED_FEE = Decimal('0.35')         # Feste eBay-Gebühr (€0,35)
    ADDITIONAL_COSTS = Decimal('1.75')       # Zusätzliche Kosten (z.B. Verpackung)

    # Gewinnmarge
    PROFIT_MARGIN_PERCENT = Decimal('0.30')  # Gewünschte Gewinnmarge in Prozent (30 %)
    MINIMUM_PROFIT = Decimal('3.00')         # Mindestgewinn (€3,00)

    # Preisgrenzen für Angebote
    OFFER_MIN_DISCOUNT = Decimal('0.10')     # Rabatt für Minimum Best Offer (10 %)
    OFFER_ACCEPT_DISCOUNT = Decimal('0.05')  # Rabatt für Auto Accept Best Offer (5%)

    # Rundung
    DECIMAL_PLACES = Decimal('0.01')         # Rundung auf zwei Dezimalstellen

    @staticmethod
    async def get_price(soup, num, db_pool):
        """
        Extrahiert den Preis und die Versandkosten eines Artikels, berechnet den Gesamtpreis
        mit Gewinnmarge und eBay-Gebühren und speichert ihn zusammen mit Best Offer Werten
        in der Datenbank.

        :param soup: BeautifulSoup-Objekt der Seite.
        :param num: ID des Artikels in der Datenbank.
        :param db_pool: Verbindung zur Datenbank.
        :return: Der berechnete Preis als String oder leerer String bei Fehlern.
        """
        try:
            # Preis extrahieren und bereinigen
            price_element = soup.find(class_="priceValue")
            if not price_element:
                logger.warning(f"Preis nicht gefunden für Artikel {num}. Standardwert (0.00 €) wird verwendet.")
                price = Decimal('0.00')
            else:
                price = PriceProcessing.clean_price(price_element.text)

            # Versandkosten extrahieren und bereinigen
            shipping_cost_element = soup.find(class_="shippingCosts")
            if not shipping_cost_element:
                logger.warning(f"Versandkosten nicht gefunden für Artikel {num}. Standardwert (0.00 €) wird verwendet.")
                shipping_cost = Decimal('0.00')
            else:
                shipping_cost = PriceProcessing.extract_shipping_cost(shipping_cost_element.text)

            # Gesamtpreis berechnen
            total_price = price + shipping_cost
            final_price = PriceProcessing.calculate_price(total_price)

            # Berechne Angebotspreise
            min_offer_price = PriceProcessing.calculate_min_offer_price(final_price)
            auto_accept_price = PriceProcessing.calculate_auto_accept_price(final_price)

            # Preise in die Datenbank speichern
            await PriceProcessing.save_price_to_db(db_pool, num, final_price, min_offer_price, auto_accept_price)

            logger.info(f"Finaler Preis für Artikel {num} berechnet und gespeichert: {final_price}")
            return str(final_price)
        except Exception as e:
            logger.exception(f"Ein unerwarteter Fehler beim Verarbeiten von Artikel {num}: {e}")
            return ''

    @staticmethod
    def clean_price(price_text):
        """
        Bereinigt und konvertiert einen Preisstring in einen Decimal-Wert.

        :param price_text: Der Preisstring (z. B. "5,00 €").
        :return: Der bereinigte Preis als Decimal.
        """
        try:
            cleaned = re.sub(r'[^\d,]', '', price_text).replace(',', '.')
            return Decimal(cleaned)
        except Exception as e:
            logger.error(f"Fehler beim Bereinigen des Preises '{price_text}': {e}")
            return Decimal('0.00')

    @staticmethod
    def extract_shipping_cost(shipping_cost_text):
        """
        Extrahiert die Versandkosten aus einem String und konvertiert sie in einen Decimal-Wert.

        :param shipping_cost_text: Der Versandkostenstring (z. B. "Versand: 2,50 €").
        :return: Die bereinigten Versandkosten als Decimal.
        """
        try:
            match = re.search(r':\s*([\d,]+)', shipping_cost_text)
            if match:
                cleaned = match.group(1).replace(',', '.')
                return Decimal(cleaned)
            else:
                logger.warning(f"Versandkosten nicht korrekt formatiert: {shipping_cost_text}. Standardwert (0.00 €) wird verwendet.")
                return Decimal('0.00')
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren der Versandkosten aus '{shipping_cost_text}': {e}")
            return Decimal('0.00')

    @staticmethod
    def calculate_price(total_price):
        """
        Berechnet den Endpreis eines Artikels basierend auf dem Gesamtpreis (inkl. Versandkosten),
        eBay-Gebühren und einer gewünschten Gewinnmarge.

        :param total_price: Der Gesamtpreis des Artikels (inkl. Versandkosten).
        :return: Der berechnete Endpreis, gerundet auf zwei Dezimalstellen.
        """
        try:
            if total_price < 0:
                raise ValueError("Der Gesamtpreis darf nicht negativ sein.")

            # eBay-Gebühren berechnen
            ebay_fees = (total_price * PriceProcessing.EBAY_PERCENTAGE_FEE) + PriceProcessing.EBAY_FIXED_FEE

            # Gesamtkosten berechnen
            total_costs = total_price + ebay_fees + PriceProcessing.ADDITIONAL_COSTS

            # Gewünschte Gewinnmarge berechnen
            desired_profit = max(total_price * PriceProcessing.PROFIT_MARGIN_PERCENT, PriceProcessing.MINIMUM_PROFIT)

            # Endpreis berechnen und runden
            final_price = (total_costs + desired_profit).quantize(PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP)

            return final_price
        except Exception as e:
            logger.error(f"Fehler in calculate_price: {e}")
            return None

    @staticmethod
    def calculate_min_offer_price(final_price):
        """
        Berechnet den minimalen Preis für ein automatisches Angebot (-10 % vom Endpreis).

        :param final_price: Der finale Artikelpreis.
        :return: Minimaler Angebotspreis.
        """
        return (final_price * (1 - PriceProcessing.OFFER_MIN_DISCOUNT)).quantize(PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP)

    @staticmethod
    def calculate_auto_accept_price(final_price):
        """
        Berechnet den Preis, zu dem Preisvorschläge automatisch akzeptiert werden (-5 % vom Endpreis).

        :param final_price: Der finale Artikelpreis.
        :return: Automatisch akzeptierter Angebotspreis.
        """
        return (final_price * (1 - PriceProcessing.OFFER_ACCEPT_DISCOUNT)).quantize(PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP)

    @staticmethod
    async def save_price_to_db(db_pool, num, final_price, min_offer_price, auto_accept_price):
        """
        Speichert den berechneten Endpreis und die Angebotspreise in der Datenbank.

        :param db_pool: Verbindung zur Datenbank.
        :param num: ID des Artikels in der Datenbank.
        :param final_price: Der zu speichernde Endpreis.
        :param min_offer_price: Der minimale Angebotspreis.
        :param auto_accept_price: Der automatisch akzeptierte Angebotspreis.
        """
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE library
                    SET Start_price = $1,
                        Minimum_Best_Offer_Price = $2,
                        Best_Offer_Auto_Accept_Price = $3
                    WHERE id = $4
                    """,
                    str(final_price), str(min_offer_price), str(auto_accept_price), num
                )
                logger.info(f"Preise erfolgreich in die Datenbank geschrieben für Artikel {num}: {final_price}, {min_offer_price}, {auto_accept_price}")
        except Exception as e:
            logger.error(f"Fehler beim Schreiben des Preises in die Datenbank für Artikel {num}: {e}")
