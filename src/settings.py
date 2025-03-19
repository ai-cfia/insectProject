from inaturalist_client import Configuration
from pydantic import computed_field
from pydantic_settings import BaseSettings


# TODO: Annotate
class Settings(BaseSettings):
    inat_host: str = "https://api.inaturalist.org/v1"
    api_request_delay: float = 3.0

    ca_nelat: float = 83.1000000
    ca_nelng: float = -50.7500000
    ca_swlat: float = 41.2833333
    ca_swlng: float = -140.8833333
    us_nelat: float = 49.3868470
    us_nelng: float = -50.9549360
    us_swlat: float = 25.4891560
    us_swlng: float = -128.8260230

    @computed_field
    @property
    def areas(self) -> dict[str, tuple[float, float, float, float]]:
        return {
            "CA": (self.ca_nelat, self.ca_nelng, self.ca_swlat, self.ca_swlng),
            "US": (self.us_nelat, self.us_nelng, self.us_swlat, self.us_swlng),
        }

    @computed_field
    @property
    def configuration(self) -> Configuration:
        return Configuration(host=self.inat_host, retries=5, debug=True)
