from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime, timezone
import random, uuid


app = FastAPI()
CITIES=[
    ("San Jose",37.3382,-121.8863),
    ("San Francisco",37.7749,-122.4194),
    ("Los Angeles",34.0522,-118.2437),
    ("Seattle",47.6062,-122.3321),
    ("New York",40.7128,-74.0060)
]

class Weather(BaseModel):
    # schema_version:int=1
    event_id:str
    event_time:str
    city:str
    lat:float
    lon:float
    temp_c:float
    humidity:float
    wind_kmh:float
    conditions:str
    # source:str="fake-api"

@app.get("/weather")
def get_weather():
    city, lat, lon = random.choice(CITIES)
    return Weather(
        event_id=str(uuid.uuid4()),
        event_time=datetime.now(timezone.utc).isoformat(),
        city=city,
        lat=lat,
        lon=lon,
        temp_c=round(random.uniform(5, 42), 1),
        humidity=round(random.uniform(0.2,0.95),2),
        wind_kmh=round(random.uniform(0,40),1),
        conditions=random.choice(["sunny","cloudy","rain","windy"])
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9001)

