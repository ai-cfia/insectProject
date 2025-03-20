from geopy.geocoders import Nominatim
from pydantic import validate_call


# TODO: would google maps be better?
@validate_call
def get_city_province_country(coord: tuple[float, float]):
    # TODO: add a timeout
    locator = Nominatim(user_agent="inectsiNatApp", timeout=None)
    try:
        location = locator.reverse(coord, language="en")
        address = location.raw["address"] if location and location.raw else {}
        city = (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("municipality")
            or address.get("county")
            or address.get("state_district")
            or ""
        )
        province, country = address.get("state", ""), address.get("country_code", "")
        return city, province, country
    except Exception as e:
        print(f"Reverse geocoding failed for {coord}: {e}")
        return "", "", ""


if __name__ == "__main__":
    # Test coordinates (latitude, longitude)
    # Run: python -m src.geo
    test_coords = [
        [40.7128, -74.0060],
        (51.5074, -0.1278),
        (35.6895, 139.6917),
        (-33.8688, 151.2093),
        (48.8566, 2.3522),
    ]

    for coord in test_coords:
        city, province, country = get_city_province_country(coord)
        print(
            f"Coordinates: {coord} -> City: {city}, Province: {province}, Country: {country}"
        )
