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
    EBAY_PERCENTAGE_FEE = Decimal('0.12')  # eBay-Provisionssatz (12%)
    EBAY_FIXED_FEE = Decimal('0.35')  # Feste eBay-Gebühr (€0,35)
    ADDITIONAL_COSTS = Decimal('1.75')  # Zusätzliche Kosten (z.B. Verpackung)

    # Gewinnmarge
    PROFIT_MARGIN_PERCENT = Decimal('0.30')  # Gewünschte Gewinnmarge in Prozent (30%)
    MINIMUM_PROFIT = Decimal('2.00')  # Mindestgewinn (mindestens 2,00 €)
    MAXIMUM_PROFIT = Decimal('5.00')  # Maximale Mindestmarge (höchstens 5,00 €)

    # Preisgrenzen für Angebote
    OFFER_MIN_DISCOUNT = Decimal('0.10')  # Rabatt für Minimum Best Offer (10%)
    OFFER_ACCEPT_DISCOUNT = Decimal('0.05')  # Rabatt für Auto Accept Best Offer (5%)
    MINIMUM_MARGIN_ALLOWED = Decimal('2.00')  # Mindestmarge für Best Offer Preise
    MIN_ACCEPT_DIFFERENCE = Decimal('0.05')  # Mindestabstand zwischen final_price und auto_accept_price

    # Rundung
    DECIMAL_PLACES = Decimal('0.01')  # Rundung auf zwei Dezimalstellen

    @staticmethod
    async def get_price(soup, num, db_pool):
        try:
            # Extrahiere den Produktpreis
            price_element = soup.find(class_="priceValue")
            price = PriceProcessing.clean_price(price_element.text) if price_element else Decimal('0.00')

            # Extrahiere die Versandkosten
            shipping_cost_element = soup.find(class_="shippingCosts")
            shipping_cost = PriceProcessing.extract_shipping_cost(
                shipping_cost_element.text) if shipping_cost_element else Decimal('0.00')

            # Berechnung der eBay-Gebühren (12% des Gesamtpreises + 0,35 € fixe Gebühr)
            ebay_fee = (price + shipping_cost) * PriceProcessing.EBAY_PERCENTAGE_FEE
            total_ebay_fee = ebay_fee + PriceProcessing.EBAY_FIXED_FEE

            # Netto-Einkaufspreis (Produkt + Versand + eBay-Gebühren)
            net_purchase_price = price + shipping_cost + total_ebay_fee

            # Berechnung des finalen Verkaufspreises
            final_price = PriceProcessing.calculate_price(net_purchase_price)

            # Dynamische Mindestmarge: 25% des Netto-Einkaufspreises
            dynamic_min_profit = net_purchase_price * Decimal("0.25")

            # Mindestmarge zwischen 2,00 € und 5,00 € begrenzen
            minimum_profit = max(PriceProcessing.MINIMUM_PROFIT,
                                 min(dynamic_min_profit, PriceProcessing.MAXIMUM_PROFIT))

            # Endgültige Marge: Höherer Wert aus gewünschter 30%-Marge oder Mindestmarge
            margin = max(net_purchase_price * PriceProcessing.PROFIT_MARGIN_PERCENT, minimum_profit)
            margin = margin.quantize(PriceProcessing.DECIMAL_PLACES, rounding=ROUND_HALF_UP)

            # Berechnung der Angebotspreise
            min_offer_price = PriceProcessing.calculate_min_offer_price(final_price)
            auto_accept_price = PriceProcessing.calculate_auto_accept_price(final_price)

            # 1️⃣ **Fix: Minimum Best Offer darf nie größer sein als Auto Accept**
            if min_offer_price > auto_accept_price:
                min_offer_price = auto_accept_price  # Setze min_offer_price auf auto_accept_price

            # Speichern der berechneten Preise in die Datenbank
            await PriceProcessing.save_price_to_db(db_pool, num, final_price, min_offer_price, auto_accept_price,
                                                   margin)

            logger.info(f"Finaler Preis für Artikel {num} berechnet und gespeichert: {final_price}, Marge: {margin}")
            return str(final_price)
        except Exception as e:
            logger.exception(f"Fehler beim Verarbeiten von Artikel {num}: {e}")
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
                logger.warning(
                    f"Versandkosten nicht korrekt formatiert: {shipping_cost_text}. Standardwert (0.00 €) wird verwendet.")
                return Decimal('0.00')
        except Exception as e:
            logger.error(f"Fehler beim Extrahieren der Versandkosten aus '{shipping_cost_text}': {e}")
            return Decimal('0.00')

    @staticmethod
    def calculate_price(net_purchase_price):
        """
        Berechnet den Endpreis eines Artikels basierend auf dem Netto-Einkaufspreis,
        eBay-Gebühren und einer gewünschten Gewinnmarge.
        """
        try:
            if net_purchase_price < 0:
                raise ValueError("Der Gesamtpreis darf nicht negativ sein.")

            total_costs = net_purchase_price + PriceProcessing.ADDITIONAL_COSTS
            desired_profit = max(net_purchase_price * PriceProcessing.PROFIT_MARGIN_PERCENT,
                                 PriceProcessing.MINIMUM_PROFIT)
            final_price = (total_costs + desired_profit).quantize(PriceProcessing.DECIMAL_PLACES,
                                                                  rounding=ROUND_HALF_UP)

            return final_price
        except Exception as e:
            logger.error(f"Fehler in calculate_price: {e}")
            return None

    @staticmethod
    def calculate_min_offer_price(final_price):
        return (final_price * (1 - PriceProcessing.OFFER_MIN_DISCOUNT)).quantize(PriceProcessing.DECIMAL_PLACES,
                                                                                 rounding=ROUND_HALF_UP)

    @staticmethod
    def calculate_auto_accept_price(final_price):
        return (final_price * (1 - PriceProcessing.OFFER_ACCEPT_DISCOUNT)).quantize(PriceProcessing.DECIMAL_PLACES,
                                                                                    rounding=ROUND_HALF_UP)

    @staticmethod
    async def save_price_to_db(db_pool, num, final_price, min_offer_price, auto_accept_price, margin):
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE library
                    SET Start_price = $1,
                        Minimum_Best_Offer_Price = $2,
                        Best_Offer_Auto_Accept_Price = $3,
                        Margin = $4
                    WHERE id = $5
                    """,
                    str(final_price), str(min_offer_price), str(auto_accept_price), str(margin), num
                )
                logger.info(f"Preise und Marge erfolgreich in die Datenbank geschrieben für Artikel {num}.")
        except Exception as e:
            logger.error(f"Fehler beim Schreiben der Preise in die Datenbank für Artikel {num}: {e}")
